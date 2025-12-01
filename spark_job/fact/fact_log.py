# spark_job/fact/fact_log.py

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from spark_job.schema import (
    log_value_schema,
    FACT_LOG_COLUMNS,
)


def parse_fact_log(kafka_df: DataFrame) -> DataFrame:
    """
    Kafka에서 읽어온 DF(key, value, topic, timestamp, ...)를
    analytics.fact_log 스키마에 맞는 DF로 변환한다.
    IO(write)는 하지 않고 변환만 담당.
    """

    parsed = (
        kafka_df
        .selectExpr(
            "CAST(value AS STRING) AS raw_json",
            "topic",
            "timestamp AS kafka_timestamp",
        )
        .withColumn(
            "json",
            F.from_json(F.col("raw_json"), log_value_schema),
        )
        .select(
            F.col("json.timestamp").alias("event_ts_str"),
            F.col("json.service").alias("service"),
            F.col("json.level").alias("level"),
            F.col("json.request_id").alias("request_id"),
            F.col("json.method").alias("method"),
            F.col("json.path").alias("path"),
            F.col("json.status_code").alias("status_code"),
            F.col("json.latency").alias("latency"),
            F.col("json.event").alias("event"),
            F.col("json.user_id").alias("user_id"),
            F.col("json.notification_type").alias("notification_type"),
            F.col("json.product_id").alias("product_id"),
            F.col("json.amount").alias("amount"),
            F.col("topic"),
            F.col("raw_json"),
            F.col("kafka_timestamp"),
        )
        .withColumn(
            "event_ts",
            F.to_timestamp("event_ts_str").cast("timestamp"),
        )
        .withColumn(
            "ingest_ts",
            F.current_timestamp(),
        )
    )

    result = parsed.select(*FACT_LOG_COLUMNS)
    return result
