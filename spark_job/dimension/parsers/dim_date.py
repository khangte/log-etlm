# spark_job/dimension/dim_date.py

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from ..schema import DIM_DATE_COLUMNS


def parse_dim_date(fact_df: DataFrame, *, time_col: str = "event_ts") -> DataFrame:
    """
    fact_event DF에서 기준 시각 컬럼으로 dim_date DF 생성.
    - 입력 DF: time_col (TimestampType) 컬럼을 포함
    - 출력 DF: dim_date 스키마에 맞는 DF (date 기준 distinct)
    """

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
