from __future__ import annotations

from pyspark.sql import types as T


DLQ_VALUE_SCHEMA: T.StructType = T.StructType(
    [
        T.StructField("error_type", T.StringType(), True),
        T.StructField("error_message", T.StringType(), True),
        T.StructField("service", T.StringType(), True),
        T.StructField("event_id", T.StringType(), True),
        T.StructField("request_id", T.StringType(), True),
        T.StructField("source_topic", T.StringType(), True),
        T.StructField("created_ms", T.LongType(), True),
        T.StructField("raw_json", T.StringType(), True),
    ]
)


FACT_EVENT_DLQ_SCHEMA: T.StructType = T.StructType(
    [
        T.StructField("ingest_ts", T.TimestampType(), False),
        T.StructField("processed_ts", T.TimestampType(), False),
        T.StructField("service", T.StringType(), True),
        T.StructField("event_id", T.StringType(), True),
        T.StructField("request_id", T.StringType(), True),
        T.StructField("source_topic", T.StringType(), True),
        T.StructField("created_ts", T.TimestampType(), True),
        T.StructField("error_type", T.StringType(), False),
        T.StructField("error_message", T.StringType(), True),
        T.StructField("raw_json", T.StringType(), False),
    ]
)


FACT_EVENT_DLQ_COLUMNS: list[str] = [
    "ingest_ts",
    "processed_ts",
    "service",
    "event_id",
    "request_id",
    "source_topic",
    "created_ts",
    "error_type",
    "error_message",
    "raw_json",
]
