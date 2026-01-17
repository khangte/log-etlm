from __future__ import annotations

from pyspark.sql import DataFrame, functions as F


def validate_event(parsed_df: DataFrame) -> tuple[DataFrame, DataFrame]:
    """유효성 기준으로 good/bad를 분리한다."""
    good_df = parsed_df.where(F.col("json").isNotNull())
    bad_df = (
        parsed_df.where(F.col("json").isNull())
        .withColumn("error_type", F.lit("json_parse_failed"))
        .withColumn("error_message", F.lit("value is not valid JSON"))
    )
    return good_df, bad_df
