# spark_job/dim/dim_time.py

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from .schema import DIM_TIME_COLUMNS


def parse_dim_time(fact_df: DataFrame) -> DataFrame:
    """
    fact_event DF에서 event_ts 기준으로 dim_time DF 생성.

    - 입력 DF: event_ts (TimestampType) 컬럼 포함
    - 출력 DF: 시간 단위 차원 테이블용 DF
        - hour       : 0~23
        - minute     : 0~59
        - second     : 0~59
        - time_of_day: 시간대 구간(dawn/morning/afternoon/evening)
    """

    kst_ts = F.from_utc_timestamp(F.col("event_ts"), "Asia/Seoul")

    base = (
        fact_df
        .select("event_ts")
        .where(F.col("event_ts").isNotNull())
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
