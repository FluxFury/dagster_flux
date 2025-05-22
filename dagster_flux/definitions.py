from dagster import Definitions
import os
from dagster_flux.jobs import (
    news_processing_job,
    ranking_job,
    mark_matches_finished_job,
)
from dagster_flux.sensors import (
    news_sensor_factory,
    matches_sensor_factory,
    rank_when_many_news,
    mark_matches_finished,
)
from dagster_flux.resources import KafkaResource

SENSOR_REPLICAS = 1
kafka_topics = os.getenv("KAFKA_TOPIC", "")
topics = kafka_topics.split(",") if kafka_topics else []

defs = Definitions(
    jobs=[news_processing_job, ranking_job, mark_matches_finished_job],
    sensors=[
        *(news_sensor_factory(i) for i in range(SENSOR_REPLICAS)),
        *(matches_sensor_factory(i) for i in range(SENSOR_REPLICAS)),
    ]
    + [rank_when_many_news, mark_matches_finished],
    resources={
        "kafka": KafkaResource(
            bootstrap_servers=[
                os.getenv("KAFKA_BOOTSTRAP_SERVER_DOCKER", "localhost:19092")
            ],
            topics=topics,
        )
    },
)
