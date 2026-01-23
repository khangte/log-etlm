from __future__ import annotations

from pyspark.sql import DataFrame, functions as F

from ..schema import log_value_schema


def parse_event(kafka_df: DataFrame) -> DataFrame:
    """Kafka 원본을 파싱된 구조로 변환한다."""
    return (
        kafka_df.selectExpr(
            "CAST(value AS STRING) AS raw_json",
            "CAST(key AS STRING) AS kafka_key",
            "topic",
            "partition",
            "offset",
            "timestamp AS kafka_ts",
        )
        .withColumn(
            "json",
            F.from_json(F.col("raw_json"), log_value_schema),
        )
        .withColumn(
            "spark_ingest_ts",
            F.current_timestamp(),
        )
    )
