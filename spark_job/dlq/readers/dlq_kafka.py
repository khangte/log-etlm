from __future__ import annotations

import os

from ...common.kafka_reader import read_kafka_stream


def read_dlq_stream(spark):
    """DLQ 토픽을 구독하는 Kafka 스트림을 만든다."""
    topic = os.getenv("SPARK_DLQ_TOPIC", "logs.dlq")
    group_id = os.getenv("SPARK_DLQ_GROUP_ID", "fact-event-dlq-stream")
    max_offsets = os.getenv("SPARK_DLQ_MAX_OFFSETS_PER_TRIGGER")
    starting_offsets = os.getenv("SPARK_STARTING_OFFSETS", "latest")
    return read_kafka_stream(
        spark,
        bootstrap=os.getenv("KAFKA_BOOTSTRAP"),
        topics=topic,
        group_id=group_id,
        starting_offsets=starting_offsets,
        max_offsets_per_trigger=max_offsets,
    )
