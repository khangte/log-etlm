from __future__ import annotations

from pyspark.sql import types as T


# DLQ Kafka value 스키마
DLQ_VALUE_SCHEMA: T.StructType = T.StructType(
    [
        T.StructField("error_type", T.StringType(), True),
        T.StructField("error_message", T.StringType(), True),
        T.StructField("service", T.StringType(), True),
        T.StructField("event_id", T.StringType(), True),
        T.StructField("request_id", T.StringType(), True),
        T.StructField("source_topic", T.StringType(), True),
        T.StructField("source_partition", T.IntegerType(), True),
        T.StructField("source_offset", T.LongType(), True),
        T.StructField("source_key", T.StringType(), True),
        T.StructField("created_ms", T.LongType(), True),
        T.StructField("raw_json", T.StringType(), True),
    ]
)

# DLQ Kafka value 컬럼 순서
DLQ_VALUE_COLUMNS: list[str] = [
    "error_type",
    "error_message",
    "service",
    "event_id",
    "request_id",
    "source_topic",
    "source_partition",
    "source_offset",
    "source_key",
    "created_ms",
    "raw_json",
]


# fact_event_dlq 컬럼 순서
FACT_EVENT_DLQ_COLUMNS: list[str] = [
    "ingest_ts",
    "processed_ts",
    "service",
    "event_id",
    "request_id",
    "source_topic",
    "source_partition",
    "source_offset",
    "source_key",
    "created_ts",
    "error_type",
    "error_message",
    "raw_json",
]
