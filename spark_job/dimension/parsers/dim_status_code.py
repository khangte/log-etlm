# 파일명 : spark_job/dimension/parsers/dim_status_code.py
# 목적   : dim_status_code 차원 테이블을 생성한다.

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from ..schema import DIM_STATUS_CODE_COLUMNS


def parse_dim_status_code(fact_df: DataFrame) -> DataFrame:
    """fact_event에서 dim_status_code를 생성한다."""

    base = (
        fact_df
        .select("status_code")
        .where(F.col("status_code").isNotNull())
        .distinct()
    )

    with_class = base.withColumn(
        "status_class",
        (F.floor(F.col("status_code") / 100) * 100).cast("int"),
    ).withColumn(
        "status_class",
        F.concat(F.col("status_class").cast("string"), F.lit("xx")),
    )

    with_flags = (
        with_class
        .withColumn(
            "is_error",
            F.when(F.col("status_class") == F.lit("5xx"), F.lit(1)).otherwise(F.lit(0)),
        )
    )

    with_desc = with_flags.withColumn(
        "description",
        F.when(F.col("status_code") == 200, F.lit("OK"))
        .when(F.col("status_code") == 201, F.lit("Created"))
        .when(F.col("status_code") == 204, F.lit("No Content"))
        .when(F.col("status_code") == 400, F.lit("Bad Request"))
        .when(F.col("status_code") == 401, F.lit("Unauthorized"))
        .when(F.col("status_code") == 403, F.lit("Forbidden"))
        .when(F.col("status_code") == 404, F.lit("Not Found"))
        .when(F.col("status_code") == 422, F.lit("Unprocessable Entity"))
        .when(F.col("status_code") == 429, F.lit("Too Many Requests"))
        .when(F.col("status_code") == 500, F.lit("Internal Server Error"))
        .when(F.col("status_code") == 502, F.lit("Bad Gateway"))
        .when(F.col("status_code") == 503, F.lit("Service Unavailable"))
        .otherwise(F.lit("Unknown"))
    )

    return with_desc.select(*DIM_STATUS_CODE_COLUMNS)
