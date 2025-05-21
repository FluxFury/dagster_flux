# sensors.py
import json
from typing import Dict, List, Any

from dagster import (
    sensor,
    RunRequest,
    DefaultSensorStatus,
    SensorEvaluationContext,
)
from kafka import KafkaConsumer

from dagster_flux.jobs import news_processing_job
from dagster_flux.resources import KafkaResource
from dagster import sensor, RunRequest, DefaultSensorStatus
from sqlalchemy import select, func
from dagster_flux.jobs import ranking_job      # см. ниже
from flux_orm.database import new_sync_session
from flux_orm.models.models import FilteredMatchInNews
from dagster_flux.utils import _make_run_key
# ----------------------------------------
#  Кэш консьюмеров (на процесс)
# ----------------------------------------
_CONSUMERS: dict[str, KafkaConsumer] = {}


def _get_consumer(context: SensorEvaluationContext,
                  kafka: KafkaResource,
                  group_id: str) -> KafkaConsumer:
    """
    Один сенсор → один KafkaConsumer.
    Ключуем словарь по context.sensor_name, а не по group_id.
    """
    key = context.sensor_name                 # 'watch_matches_0', 'watch_matches_1', …
    consumer = _CONSUMERS.get(key)
    if consumer is None:
        consumer = kafka.get_consumer(
            group_id=group_id,
            # client_id полезно для дебага, но не обязательно
            # client_id=f"{key}_{os.getpid()}",
        )
        _CONSUMERS[key] = consumer
    return consumer

# ----------------------------------------
#  Параметры
# ----------------------------------------
NEWS_MAX_POLL       = 50      # сколько новостей тянуть за один тик
MATCH_BATCH_SIZE    = 15      # «каждые 5 матчей»
TICK_EVERY_SECONDS  = 20      # для обоих сенсоров


def build_run_config(raw_news_batch: List[dict] | None = None,
                     matches_batch:  List[dict] | None = None) -> dict:
    """
    Возвращает run_config, в котором ВСЕ обязательные опы присутствуют.
    Если данных нет – кладём [].
    """
    return {
        "ops": {
            "read_raw_news_from_kafka_op": {
                "config": {"raw_news_batch": raw_news_batch or []}
            },
            "extract_matches_op": {
                "config": {"matches_batch": matches_batch or []}
            },
        }
    }

# --------------------------------------------------------
#  1. Фабрика сенсоров на топик raw_news
#     Один Kafka-message → один Run
# --------------------------------------------------------
def news_sensor_factory(replica_id: int):
    @sensor(
        name=f"watch_raw_news_{replica_id}",
        job_name=news_processing_job.name,
        minimum_interval_seconds=TICK_EVERY_SECONDS,
        default_status=DefaultSensorStatus.RUNNING,
    )
    def watch_raw_news(context: SensorEvaluationContext, kafka: KafkaResource):
        group_id = "dagster_flux_news_sensor"
        consumer = _get_consumer(context, kafka, group_id)
        polled = consumer.poll(max_records=NEWS_MAX_POLL, timeout_ms=2_000)

        for tp, records in polled.items():
            if tp.topic != "CS2_raw_news":
                continue

            for msg in records:
                try:
                    payload: dict[str, Any] = json.loads(msg.value.decode())
                except json.JSONDecodeError:
                    context.log.warning("Skip malformed news message")
                    continue

                yield RunRequest(
                    run_key=f"raw_news:{msg.offset}",
                    run_config=build_run_config(raw_news_batch=[payload])
                )

        if polled:
            consumer.commit()
    return watch_raw_news


# --------------------------------------------------------
#  2. Фабрика сенсоров на топик match
#     Копим 5 сообщений → один Run
# --------------------------------------------------------
def matches_sensor_factory(replica_id: int):
    @sensor(
        name=f"watch_matches_{replica_id}",
        job_name=news_processing_job.name,
        minimum_interval_seconds=TICK_EVERY_SECONDS,
        default_status=DefaultSensorStatus.RUNNING,
    )
    def watch_matches(context: SensorEvaluationContext, kafka: KafkaResource):
        group_id = "dagster_flux_match_sensor"
        consumer = _get_consumer(context, kafka, group_id)

        # курсор хранит уже накопленные (но ещё не отправленные) матчи
        accumulated: List[Dict[str, Any]] = json.loads(context.cursor or "[]")

        need = MATCH_BATCH_SIZE - len(accumulated)
        if need > 0:
            polled = consumer.poll(max_records=need, timeout_ms=2_000)
            for tp, records in polled.items():
                if tp.topic != "CS2_match":
                    continue
                for msg in records:
                    try:
                        accumulated.append(json.loads(msg.value.decode()))
                    except json.JSONDecodeError:
                        context.log.warning("Skip malformed match message")

        # если всё ещё меньше пяти ― только обновляем курсор и ждём
        if len(accumulated) < MATCH_BATCH_SIZE:
            context.update_cursor(json.dumps(accumulated))
            return
        context.log.info(f"Найдено {len(accumulated)} матчей для ранжирования")
        # готов батч из пяти матчей
        yield RunRequest(
            run_key=_make_run_key(accumulated),
            run_config=build_run_config(matches_batch=accumulated)
        )

        # сбрасываем буфер, коммитим offsets
        context.update_cursor(json.dumps([]))
        consumer.commit()
    return watch_matches





THRESHOLD = 10               # сколько новостей надо на матч

@sensor(
    name="rank_when_many_news",
    job_name=ranking_job.name,
    minimum_interval_seconds=60,
    default_status=DefaultSensorStatus.RUNNING,
)
def rank_when_many_news(context: SensorEvaluationContext):
    def _get_ready_matches():
        with new_sync_session() as ses:
            rows = ses.execute(
                select(FilteredMatchInNews.match_id)
                .where(FilteredMatchInNews.respective_relevance == None)  # ещё не ранжировали
                .group_by(FilteredMatchInNews.match_id)
                .having(func.count() >= THRESHOLD)
            ).scalars().all()  
            return rows

    match_ids = _get_ready_matches()
    
    context.log.info(f"Найдено {len(match_ids)} матчей для ранжирования")

    for mid in match_ids:
        yield RunRequest(
            run_key=f"rank:{mid}",
            run_config={"ops": {"rank_news_by_llm_op": {"config": {"match_id": mid}}}},
        )