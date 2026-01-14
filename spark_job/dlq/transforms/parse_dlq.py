from __future__ import annotations

from pyspark.sql import DataFrame, functions as F

from ..schema import DLQ_VALUE_SCHEMA, FACT_EVENT_DLQ_COLUMNS


def parse_dlq(dlq_source: DataFrame) -> DataFrame:
    """
    Kafka logs.dlq raw DF -> fact_event_dlq 스키마로 변환한다.
    """
    dlq_parsed = (
        dlq_source.selectExpr(
            "CAST(value AS STRING) AS raw_json",
            "topic",
            "timestamp AS kafka_ts",
        )
        .withColumn("json", F.from_json(F.col("raw_json"), DLQ_VALUE_SCHEMA))
    )
    dlq_df = dlq_parsed.select(
        F.col("kafka_ts").alias("ingest_ts"),
        F.current_timestamp().alias("processed_ts"),
        F.col("json.service").alias("service"),
        F.col("json.event_id").alias("event_id"),
        F.col("json.request_id").alias("request_id"),
        F.coalesce(F.col("json.source_topic"), F.col("topic")).alias("source_topic"),
        F.expr("timestamp_millis(json.created_ms)").alias("created_ts"),
        F.coalesce(F.col("json.error_type"), F.lit("unknown")).alias("error_type"),
        F.coalesce(F.col("json.error_message"), F.lit("")).alias("error_message"),
        F.coalesce(F.col("json.raw_json"), F.col("raw_json")).alias("raw_json"),
    )
    return dlq_df.select(*FACT_EVENT_DLQ_COLUMNS)
