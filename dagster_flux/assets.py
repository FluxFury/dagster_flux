from dagster import (
    AssetSelection,
    Config,
    MaterializeResult,
    OpExecutionContext,
    asset,
    define_asset_job,
)


class KafkaConsumerConfig(Config):
    max_offset: dict[str, int]
    topic_to_messages: dict[str, list[str]]  # Map topics to their messages


@asset
def loaded_from_kafka(
    context: OpExecutionContext, config: KafkaConsumerConfig
) -> MaterializeResult:
    # Get all messages across all topics
    all_messages = []
    for topic, messages in config.topic_to_messages.items():
        context.log.info(f"Processing {len(messages)} messages from topic {topic}")
        all_messages.extend(messages)

    context.log.info(f"Handling kafka batch with {len(all_messages)} total messages")

    # write file with records, partitioned by min/max batch ids
    with open(
        f"data/{'-'.join(f'{t}_{o}' for t, o in config.max_offset.items())}", "w"
    ) as f:
        f.writelines(all_messages)

    return MaterializeResult(
        metadata={
            "kafka_batch_size": len(all_messages),
            "kafka_batch_value_start": all_messages[0] if all_messages else "",
            "kafka_batch_value_end": all_messages[-1] if all_messages else "",
            "topics_processed": list(config.topic_to_messages.keys()),
        }
    )


kafka_consumer_output_job = define_asset_job(
    name="kafka_consumer_output", selection=AssetSelection.assets(loaded_from_kafka)
)
