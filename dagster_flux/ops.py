from dagster import op, In, Out
from typing import Any
from flux_orm.database import new_session
from dagster_flux.openai_client import (
    summarize_news_and_extract_keywords,
    determine_match_to_news_relevance,
    rank_news_by_llm,
)
from flux_orm.models.models import FormattedNews
from dagster_flux.utils import check_news_relevance, prepare_match_to_llm, get_semaphore
from sqlalchemy import select
from flux_orm.models.models import Match, Team
from sqlalchemy.orm import joinedload
from flux_orm.models.models import FilteredMatchInNews, PipelineStatus, MatchStatus
from flux_orm.models.enums import MatchStatusEnum
from flux_orm.models.utils import utcnow_naive
from typing import Sequence
from flux_orm.models.utils import model_to_dict


@op(
    config_schema={"raw_news_batch": list},  # предполагаем, что сюда придёт список dict
    out=Out(list[dict[str, Any]]),
    description="Берёт батч новостей (уже считанных сенсором) из run_config.",
)
async def read_raw_news_from_kafka_op(context) -> list[dict[str, Any]]:
    raw_news_batch = context.op_config["raw_news_batch"]
    context.log.info(
        f"[read_raw_news_from_kafka_op] Получено {len(raw_news_batch)} новостей из сенсора."
    )
    return raw_news_batch


@op(
    ins={"raw_news_list": In(list[dict[str, Any]])},
    out=Out(list[dict[str, Any]]),
    description="Обработка (Summarize & Format) сырой новости с помощью асинхронного LLM, в т.ч. извлечение keywords.",
)
async def summarize_and_format_news_op(
    context, raw_news_list: list[dict[str, Any]]
) -> list[dict[str, Any]]:
    formatted_news_list = []
    for news in raw_news_list:
        # Пример ассинхронного вызова LLM
        async with get_semaphore():
            summary, keywords = await summarize_news_and_extract_keywords(
                news.get("text")
            )
        # Пример “форматирования” (реально – вызываете свой LLM):
        formatted_news = {
            "sport_id": news.get("sport_id"),
            "header": news.get("header"),
            "text": summary,
            "keywords": keywords,
            "url": news.get("url"),
            "news_creation_time": news.get("news_creation_time"),
        }
        formatted_news_list.append(formatted_news)
    context.log.info(
        f"[summarize_and_format_news_op] Обработано {len(formatted_news_list)} новостей."
    )
    return formatted_news_list


@op(
    ins={"formatted_news_list": In(list[dict[str, Any]])},
    out=Out(list[dict[str, Any]]),
    description="Сохранение отформатированных новостей (FormattedNews) в БД (ассинхронно) + возврат того же списка.",
)
async def store_formatted_news_in_db_op(
    context, formatted_news_list: list[dict[str, Any]]
) -> list[dict[str, Any]]:
    async with new_session() as session:
        for fn in formatted_news_list:
            new_obj = FormattedNews(
                sport_id=fn.get("sport_id"),
                header=fn.get("header"),
                text=fn.get("text"),
                keywords=fn.get("keywords"),
                url=fn.get("url"),
                news_creation_time=utcnow_naive(),
            )
            session.add(new_obj)
        await session.commit()

    context.log.info(
        f"[store_formatted_news_in_db_op] Сохранено {len(formatted_news_list)} записей."
    )
    return formatted_news_list


@op(
    config_schema={"matches_batch": list},
    out=Out(list[dict[str, Any]]),
    description="Берёт батч матчей, пришедший от сенсора.",
)
async def extract_matches_op(context) -> list[dict[str, Any]]:
    return context.op_config["matches_batch"]


async def _get_formatted_news_list() -> Sequence[FormattedNews]:
    async with new_session() as session:
        stmt = select(FormattedNews)
        news = (await session.execute(stmt)).scalars().all()
        return news


def _news_key(news: dict[str, Any]) -> str:
    return news.get("formatted_news_id") or news["url"]


@op(
    ins={
        "matches_list": In(list[dict[str, Any]]),
        "formatted_news_list": In(list[dict[str, Any]]),
    },
    out=Out(list[dict[str, Any]], is_required=False),
    description="Пример асинхронного пересечения (match x news).",
)
async def cross_news_with_matches_op(
    context,
    matches_list: list[dict[str, Any]],
    formatted_news_list: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    news_dict = {
        _news_key(model_to_dict(n)): model_to_dict(n)
        for n in await _get_formatted_news_list()
    }

    # 2) новости текущего батча (может ещё без formatted_news_id)
    for n in formatted_news_list:
        news_dict[_news_key(n)] = n

    # 3) декарт × без двойников
    cross_result = [
        {"match": match, "news": news}
        for match in matches_list
        for news in news_dict.values()
    ]

    if len(cross_result) == 0:
        return []

    context.log.info(f"Pairs generated: {len(cross_result)}")

    return cross_result


@op(
    ins={"match_news_pairs": In(list[dict[str, Any]])},
    out=Out(list[dict[str, Any]], is_required=False),
    description="Асинхронная фильтрация пар (match, news) с помощью алгоритма.",
)
async def filter_pairs_by_algorithm_op(
    context, match_news_pairs: list[dict[str, Any]]
) -> list[dict[str, Any]]:
    filtered_pairs = []
    for pair in match_news_pairs:
        context.log.info(f"алгоритм фильтрация пары: {pair}")
        if check_news_relevance(pair["news"]):
            filtered_pairs.append(pair)

    if not filtered_pairs:  # len == 0
        return []
    context.log.info(
        f"[filter_pairs_by_algorithm_op] Осталось {len(filtered_pairs)} пар после алгоритмической фильтрации."
    )
    return filtered_pairs


@op(
    ins={"match_news_pairs": In(list[dict[str, Any]])},
    out=Out(list[dict[str, Any]], is_required=False),
    description="Асинхронная фильтрация пар (match, news) с помощью LLM.",
)
async def filter_pairs_by_llm_op(
    context, match_news_pairs: list[dict[str, Any]]
) -> list[dict[str, Any]]:
    filtered_pairs = []
    async with new_session() as session:
        for pair in match_news_pairs:
            match_stmt = (
                select(Match)
                .options(
                    joinedload(Match.match_teams).joinedload(Team.members),
                    joinedload(Match.competition),
                )
                .where(Match.match_id == pair["match"]["match_id"])
            )
            match = (await session.execute(match_stmt)).unique().scalar_one()
            pair["match"] = prepare_match_to_llm(match)
            context.log.info(f"ллм фильтрация пары: {pair}")
            async with get_semaphore():
                if await determine_match_to_news_relevance(pair):
                    filtered_pairs.append(pair)
    if not filtered_pairs:
        return []
    context.log.info(
        f"[filter_pairs_by_llm_op] Осталось {len(filtered_pairs)} пар после LLM-фильтра."
    )
    return filtered_pairs


@op(
    ins={"filtered_pairs": In(list[dict[str, Any]])},
    description="Сохранение (match_id, news_id) в таблицу filtered_match_in_news (асинхронно).",
)
async def store_filtered_match_in_news_op(
    context, filtered_pairs: list[dict[str, Any]]
):
    async with new_session() as session:
        for pair in filtered_pairs:
            match_id, news_id = (
                pair["match"]["match_id"],
                pair["news"]["formatted_news_id"],
            )
            match_stmt = (
                select(Match)
                .options(joinedload(Match.formatted_news))
                .where(Match.match_id == match_id)
            )
            match = (await session.execute(match_stmt)).unique().scalar_one()
            news_stmt = select(FormattedNews).where(
                FormattedNews.formatted_news_id == news_id
            )
            news = (await session.execute(news_stmt)).scalar_one()
            if news not in match.formatted_news:
                match.formatted_news.append(news)

        await session.commit()

    context.log.info("[store_filtered_match_in_news_op] Данные сохранены.")


@op(
    config_schema={"match_id": str},
    description="Пакетное ранжирование новостей для одного матча",
)
async def rank_news_by_llm_op(context):
    match_id = context.op_config["match_id"]
    async with new_session() as ses:
        rows = await ses.execute(
            select(FilteredMatchInNews).where(
                FilteredMatchInNews.match_id == match_id,
                FilteredMatchInNews.respective_relevance == None,
            )
        )
        pairs = rows.scalars().all()
        match_stmt = (
            select(Match)
            .options(
                joinedload(Match.match_teams).joinedload(Team.members),
                joinedload(Match.competition),
            )
            .where(Match.match_id == match_id)
        )
        match_obj = (await ses.execute(match_stmt)).unique().scalar_one()
        match_to_llm = prepare_match_to_llm(match_obj)

        news_to_llm_list: list[dict[str, Any]] = []

        for pair in pairs:
            news_stmt = select(FormattedNews).where(
                FormattedNews.formatted_news_id == pair.news_id
            )
            news_obj = (await ses.execute(news_stmt)).scalar_one()
            news_to_llm = {
                "news_id": pair.news_id,
                "header": news_obj.header,
                "text": news_obj.text,
            }
            news_to_llm_list.append(news_to_llm)

        async with get_semaphore():
            rankings: dict[str, int] = await rank_news_by_llm(
                match_to_llm, news_to_llm_list
            )
        for pair in pairs:
            pair.respective_relevance = rankings[str(pair.news_id)]

        match_obj.pipeline_update_time = utcnow_naive()
        try:
            await ses.commit()
        except Exception as e:
            context.log.error(f"Ошибка при сохранении данных: {e}")
            await ses.rollback()

    context.log.info(f"Ранжировано {len(pairs)} новостей для матча {match_id}.")


@op(
    config_schema={"status_id": str},
    description="Метка матча как прошедшего",
)
async def mark_match_finished_op(context):
    status_id = context.op_config["status_id"]
    async with new_session() as ses:
        match_stmt = select(MatchStatus).where(MatchStatus.status_id == status_id)
        match_status = (await ses.execute(match_stmt)).unique().scalar_one()
        match_status.name = MatchStatusEnum.FINISHED
        await ses.commit()
