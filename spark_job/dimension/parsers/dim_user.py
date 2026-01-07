# spark_job/dimension/dim_user.py

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from ..schema import DIM_USER_COLUMNS


def parse_dim_user(fact_df: DataFrame) -> DataFrame:
    """
    fact_event DF에서 user_id 기준으로 dim_user DF 생성.
    - 입력 DF: user_id (StringType) 컬럼 포함
    - 출력 DF: user_id 기준 distinct + is_active 기본값
    """

    base = (
        fact_df
        .select("user_id")
        .where(F.col("user_id").isNotNull())
        .distinct()
    )

    enriched = (
        base
        .withColumn("is_active", F.lit(1).cast("int"))
        .withColumn("description", F.lit(None).cast("string"))
    )

    return enriched.select(*DIM_USER_COLUMNS)
