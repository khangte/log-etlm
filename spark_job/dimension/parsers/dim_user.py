# 파일명 : spark_job/dimension/parsers/dim_user.py
# 목적   : dim_user 차원 테이블을 생성한다.

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from ..schema import DIM_USER_COLUMNS


def parse_dim_user(fact_df: DataFrame) -> DataFrame:
    """fact_event에서 dim_user를 생성한다."""

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
