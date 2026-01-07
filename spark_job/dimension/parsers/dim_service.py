# spark_job/dimension/dim_service.py

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from ..schema import DIM_SERVICE_COLUMNS


def parse_dim_service(fact_df: DataFrame) -> DataFrame:
    """
    fact_event DF에서 service 기준으로 dim_service DF 생성.
    - 입력 DF: service (StringType) 컬럼 포함
    - 출력 DF: service 기준 distinct + 기본값 service_group / is_active
    """

    base = (
        fact_df
        .select("service")
        .where(F.col("service").isNotNull())
        .distinct()
    )

    enriched = (
        base
        .withColumn("service_group", F.lit("default"))
        .withColumn("is_active", F.lit(1).cast("int"))
        .withColumn("description", F.lit(None).cast("string"))
    )

    return enriched.select(*DIM_SERVICE_COLUMNS)
