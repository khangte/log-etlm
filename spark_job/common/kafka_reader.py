from __future__ import annotations

from typing import Iterable

from pyspark.sql import DataFrame, SparkSession


def normalize_topics(topics: str | Iterable[str]) -> str:
    """subscribe 옵션에 맞게 토픽 목록을 정규화한다."""
    if isinstance(topics, str):
        items = [t.strip() for t in topics.split(",")]
    else:
        items = [str(t).strip() for t in topics]
    return ",".join([t for t in items if t])


def read_kafka_stream(
    spark: SparkSession,
    *,
    bootstrap: str,
    topics: str,
    group_id: str,
    starting_offsets: str,
    max_offsets_per_trigger: str | None,
    fail_on_data_loss: str = "false",
) -> DataFrame:
    """Kafka 스트림을 읽는 공통 reader를 제공한다."""
    reader = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", bootstrap)
        .option("subscribe", normalize_topics(topics))
        .option("startingOffsets", starting_offsets)
        .option("kafka.group.id", group_id)
        .option("failOnDataLoss", fail_on_data_loss)
    )
    if max_offsets_per_trigger:
        reader = reader.option("maxOffsetsPerTrigger", max_offsets_per_trigger)
    return reader.load()
