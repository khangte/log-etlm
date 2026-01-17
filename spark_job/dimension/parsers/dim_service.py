# 파일명 : spark_job/dimension/parsers/dim_service.py
# 목적   : dim_service 차원 테이블을 생성한다.

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from ..schema import DIM_SERVICE_COLUMNS


def parse_dim_service(
    fact_df: DataFrame,
    *,
    service_map_df: DataFrame | None = None,
) -> DataFrame:
    """fact_event에서 dim_service를 생성한다."""

    base = (
        fact_df
        .select("service")
        .where(F.col("service").isNotNull())
        .distinct()
    )

    if service_map_df is None:
        enriched = (
            base
            .withColumn("service_group", F.lit("default"))
            .withColumn("is_active", F.lit(1).cast("int"))
            .withColumn("description", F.lit(None).cast("string"))
        )
    else:
        mapped = base.join(service_map_df, on="service", how="left")
        enriched = (
            mapped
            .withColumn(
                "service_group",
                F.coalesce(F.col("service_group"), F.lit("default")),
            )
            .withColumn(
                "is_active",
                F.coalesce(F.col("is_active"), F.lit(1)).cast("int"),
            )
            .withColumn("description", F.col("description").cast("string"))
        )

    return enriched.select(*DIM_SERVICE_COLUMNS)
