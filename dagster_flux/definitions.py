from dagster import Definitions
import os
from dagster_flux.jobs import news_processing_job, ranking_job
from dagster_flux.sensors import news_sensor_factory, matches_sensor_factory, rank_when_many_news
from dagster_flux.resources import KafkaResource
from dagster_flux.assets import loaded_from_kafka
from dagster_flux.assets import kafka_consumer_output_job

SENSOR_REPLICAS = 1
kafka_topics = os.getenv("KAFKA_TOPIC", "")
topics = kafka_topics.split(",") if kafka_topics else []

defs = Definitions(
    assets=[loaded_from_kafka],
    jobs=[news_processing_job, kafka_consumer_output_job, ranking_job],
    sensors=[

        *(news_sensor_factory(i) for i in range(SENSOR_REPLICAS)),

        *(matches_sensor_factory(i) for i in range(SENSOR_REPLICAS)),
    ] + [rank_when_many_news],
    resources={
        "kafka": KafkaResource(
            bootstrap_servers=[os.getenv("KAFKA_BOOTSTRAP_SERVER_LOCAL", "localhost:19092")],
            topics=topics
        )
    },
)
