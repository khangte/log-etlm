# spark_job/dimension/dim_time.py

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T

from spark_job.schema import DIM_TIME_COLUMNS


def parse_dim_time(fact_df: DataFrame) -> DataFrame:
    """
    fact_log DF에서 event_ts 기준으로 dim_time DF 생성.

    - 입력 DF: event_ts (TimestampType) 컬럼 포함
    - 출력 DF: 시간 단위 차원 테이블용 DF
        - time_key   : HHMM (예: 9:30 -> 930)
        - hour       : 0~23
        - minute     : 0~59
        - second     : 0~59
        - time_of_day: 시간대 구간(dawn/morning/afternoon/evening)
    """

    base = (
        fact_df
        .select("event_ts")
        .where(F.col("event_ts").isNotNull())
        .withColumn("hour", F.hour("event_ts").cast("int"))
        .withColumn("minute", F.minute("event_ts").cast("int"))
        .withColumn("second", F.second("event_ts").cast("int"))
    )

    with_key = base.withColumn(
        "time_key",
        (F.col("hour") * F.lit(100) + F.col("minute")).cast("int"),
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

    result = fact_df.sparkSession.createDataFrame(distinct_df.rdd, schema=DIM_TIME_SCHEMA)
    return result
