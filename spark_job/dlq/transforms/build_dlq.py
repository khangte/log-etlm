from __future__ import annotations

from pyspark.sql import DataFrame, functions as F

from ..schema import FACT_EVENT_DLQ_COLUMNS


def build_dlq_df(bad_df: DataFrame) -> DataFrame:
    """bad_df + error info -> dlq_df 생성."""
    dlq_df = (
        bad_df.select(
            F.col("kafka_ts").alias("ingest_ts"),
            F.current_timestamp().alias("processed_ts"),
            F.lit(None).cast("string").alias("service"),
            F.lit(None).cast("string").alias("event_id"),
            F.lit(None).cast("string").alias("request_id"),
            F.col("topic").alias("source_topic"),
            F.lit(None).cast("timestamp").alias("created_ts"),
            F.coalesce(F.col("error_type"), F.lit("unknown")).alias("error_type"),
            F.coalesce(F.col("error_message"), F.lit("")).alias("error_message"),
            F.col("raw_json").alias("raw_json"),
        )
    )
    return dlq_df.select(*FACT_EVENT_DLQ_COLUMNS)
