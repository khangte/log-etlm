from __future__ import annotations

from pyspark.sql import DataFrame, functions as F

from ..schema import DLQ_VALUE_COLUMNS


def build_dlq_kafka_df(bad_df: DataFrame) -> DataFrame:
    """
    파싱/검증 실패 레코드를 DLQ Kafka payload 스키마로 변환한다.
    """
    processed_ts = F.current_timestamp()
    created_ms = (F.col("kafka_ts").cast("double") * F.lit(1000)).cast("long")
    event_id = F.when(
        F.col("topic").isNotNull()
        & F.col("partition").isNotNull()
        & F.col("offset").isNotNull(),
        F.concat_ws(
            "-",
            F.col("topic"),
            F.col("partition").cast("string"),
            F.col("offset").cast("string"),
        ),
    )
    event_id = F.coalesce(event_id, F.sha2(F.col("raw_json"), 256), F.lit("unknown"))
    dlq_payload = bad_df.select(
        F.coalesce(F.col("error_type"), F.lit("unknown")).alias("error_type"),
        F.coalesce(F.col("error_message"), F.lit("")).alias("error_message"),
        F.lit(None).cast("string").alias("service"),
        event_id.alias("event_id"),
        F.lit(None).cast("string").alias("request_id"),
        F.col("topic").alias("source_topic"),
        F.col("partition").cast("int").alias("source_partition"),
        F.col("offset").cast("long").alias("source_offset"),
        F.col("kafka_key").alias("source_key"),
        created_ms.alias("created_ms"),
        F.col("raw_json").alias("raw_json"),
    )
    return dlq_payload.select(*DLQ_VALUE_COLUMNS)
