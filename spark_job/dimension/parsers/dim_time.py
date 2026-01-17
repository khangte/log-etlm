# 파일명 : spark_job/dimension/parsers/dim_time.py
# 목적   : dim_time 차원 테이블을 생성한다.

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from ..schema import DIM_TIME_COLUMNS


def parse_dim_time(fact_df: DataFrame, *, time_col: str = "event_ts") -> DataFrame:
    """fact_event에서 dim_time을 생성한다."""

    kst_ts = F.from_utc_timestamp(F.col(time_col), "Asia/Seoul")

    base = (
        fact_df
        .select(F.col(time_col))
        .where(F.col(time_col).isNotNull())
        .withColumn("hour", F.hour(kst_ts).cast("int"))
        .withColumn("minute", F.minute(kst_ts).cast("int"))
        .withColumn("second", F.second(kst_ts).cast("int"))
    )

    with_key = base.withColumn(
        "time_key",
        (F.col("hour") * 10000 + F.col("minute") * 100 + F.col("second")).cast("int"),
    )

    with_bucket = with_key.withColumn(
        "time_of_day",
        F.when(F.col("hour").between(0, 5), F.lit("dawn"))
        .when(F.col("hour").between(6, 11), F.lit("morning"))
        .when(F.col("hour").between(12, 17), F.lit("afternoon"))
        .otherwise(F.lit("evening")),
    )

    distinct_df = (
        with_bucket
        .select("time_key", "hour", "minute", "second", "time_of_day")
        .distinct()
    )

    return distinct_df.select(*DIM_TIME_COLUMNS)
