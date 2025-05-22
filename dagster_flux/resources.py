from dagster import ConfigurableResource
from kafka import KafkaConsumer


class KafkaResource(ConfigurableResource):
    bootstrap_servers: list[str]
    topics: list[str]

    def get_consumer(self, group_id: str):
        return KafkaConsumer(
            *self.topics,
            bootstrap_servers=self.bootstrap_servers,
            fetch_min_bytes=4000,
            auto_offset_reset="earliest",
            enable_auto_commit=False,
            group_id=group_id,
        )
