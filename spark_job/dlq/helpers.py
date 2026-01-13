from __future__ import annotations

import os

from pyspark.sql import functions as F

from .schema import FACT_EVENT_DLQ_COLUMNS


def align_dlq_columns(df):
    """DLQ 스키마 컬럼을 누락 없이 맞춘다."""
    for col_name in FACT_EVENT_DLQ_COLUMNS:
        if col_name not in df.columns:
            df = df.withColumn(col_name, F.lit(None))
    return df.select(*FACT_EVENT_DLQ_COLUMNS)


def get_dlq_mode() -> str:
    """DLQ 저장 경로를 반환한다."""
    mode = os.getenv("SPARK_DLQ_MODE", "kafka").strip().lower()
    if mode in ("direct", "clickhouse", "ch"):
        return "direct"
    if mode in ("kafka", "topic"):
        return "kafka"
    return "kafka"


def _build_dlq_kafka_df(dlq_df):
    """DLQ 표준 스키마를 Kafka(value) 페이로드로 변환한다."""
    created_ms = F.when(
        F.col("created_ts").isNotNull(),
        (F.col("created_ts").cast("long") * F.lit(1000)),
    ).otherwise(F.col("ingest_ts").cast("long") * F.lit(1000))
    payload = (
        dlq_df.withColumn("created_ms", created_ms)
        .withColumn("key", F.coalesce(F.col("request_id"), F.col("event_id")))
        .withColumn(
            "value",
            F.to_json(
                F.struct(
                    F.col("error_type"),
                    F.col("error_message"),
                    F.col("service"),
                    F.col("event_id"),
                    F.col("request_id"),
                    F.col("source_topic"),
                    F.col("created_ms"),
                    F.col("raw_json"),
                )
            ),
        )
        .selectExpr("CAST(key AS STRING) AS key", "CAST(value AS STRING) AS value")
    )
    return payload


def publish_dlq_to_kafka(dlq_df, dlq_topic: str, kafka_bootstrap: str) -> None:
    """DLQ 메시지를 logs.dlq 토픽으로 발행한다."""
    if not dlq_df.take(1):
        return
    _build_dlq_kafka_df(dlq_df).write.format("kafka").option(
        "kafka.bootstrap.servers",
        kafka_bootstrap,
    ).option(
        "topic",
        dlq_topic,
    ).save()
