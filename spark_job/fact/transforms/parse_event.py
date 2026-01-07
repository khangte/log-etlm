from __future__ import annotations

from pyspark.sql import DataFrame, functions as F

from .schema import log_value_schema


def parse_event(kafka_df: DataFrame) -> DataFrame:
    """
    Kafka raw DF(value/topic/timestamp) -> parsed struct(json)까지 변환한다.
    """
    return (
        kafka_df.selectExpr(
            "CAST(value AS STRING) AS raw_json",
            "topic",
            "partition",
            "offset",
            "timestamp AS kafka_ts",
        )
        .withColumn(
            "json",
            F.from_json(F.col("raw_json"), log_value_schema),
        )
    )
