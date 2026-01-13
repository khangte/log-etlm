from __future__ import annotations

import os

from ...common.kafka_reader import read_kafka_stream


def read_fact_stream(spark):
    """정상 이벤트 토픽을 구독하는 Kafka 스트림을 만든다."""
    topics = os.getenv("SPARK_FACT_TOPICS", "logs.auth,logs.order,logs.payment")
    group_id = os.getenv("SPARK_FACT_GROUP_ID", "fact-event-stream")
    max_offsets = os.getenv("SPARK_MAX_OFFSETS_PER_TRIGGER", "250000")
    starting_offsets = os.getenv("SPARK_STARTING_OFFSETS", "latest")
    return read_kafka_stream(
        spark,
        bootstrap=os.getenv("KAFKA_BOOTSTRAP"),
        topics=topics,
        group_id=group_id,
        starting_offsets=starting_offsets,
        max_offsets_per_trigger=max_offsets,
    )
