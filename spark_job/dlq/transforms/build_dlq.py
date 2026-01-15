from __future__ import annotations

from pyspark.sql import DataFrame, functions as F

from ..schema import FACT_EVENT_DLQ_COLUMNS


def build_dlq_df(bad_df: DataFrame) -> DataFrame:
    """
    파싱/검증 실패 레코드를 DLQ 스키마로 변환한다.
    """
    processed_ts = F.current_timestamp()
    dlq_df = (
        bad_df.select(
            F.col("kafka_ts").alias("ingest_ts"),
            processed_ts.alias("processed_ts"),
            F.lit(None).cast("string").alias("service"),
            F.lit(None).cast("string").alias("event_id"),
            F.lit(None).cast("string").alias("request_id"),
            F.col("topic").alias("source_topic"),
            F.col("partition").cast("int").alias("source_partition"),
            F.col("offset").cast("long").alias("source_offset"),
            F.col("kafka_key").alias("source_key"),
            F.lit(None).cast("timestamp").alias("created_ts"),
            F.coalesce(F.col("error_type"), F.lit("unknown")).alias("error_type"),
            F.coalesce(F.col("error_message"), F.lit("")).alias("error_message"),
            F.col("raw_json").alias("raw_json"),
        )
    )
    return dlq_df.select(*FACT_EVENT_DLQ_COLUMNS)
