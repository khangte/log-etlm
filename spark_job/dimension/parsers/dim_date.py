# 파일명 : spark_job/dimension/parsers/dim_date.py
# 목적   : dim_date 차원 테이블을 생성한다.

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from ..schema import DIM_DATE_COLUMNS


def parse_dim_date(fact_df: DataFrame, *, time_col: str = "event_ts") -> DataFrame:
    """fact_event에서 dim_date를 생성한다."""

    base = (
        fact_df
        .select(F.to_date(F.col(time_col)).alias("date"))
        .where(F.col("date").isNotNull())
        .distinct()
    )

    enriched = (
        base
        .withColumn("year", F.year("date").cast("int"))
        .withColumn("month", F.month("date").cast("int"))
        .withColumn("day", F.dayofmonth("date").cast("int"))
        .withColumn("week", F.weekofyear("date").cast("int"))
        .withColumn("day_of_week", F.dayofweek("date").cast("int"))
        .withColumn(
            "is_weekend",
            F.when(F.col("day_of_week").isin(1, 7), F.lit(1)).otherwise(F.lit(0)),
        )
    )

    return enriched.select(*DIM_DATE_COLUMNS)
