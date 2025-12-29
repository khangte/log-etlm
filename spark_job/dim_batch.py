# spark_job/dim_batch.py
# 하루 1회 배치로 Dimension 테이블을 갱신한다.
# NOTE: 이 파일은 자동 실행되지 않으며, 수동 또는 크론으로 실행해야 한다.

from __future__ import annotations

import os
from pyspark.sql import SparkSession

from .dimension import (
    parse_dim_date,
    parse_dim_service,
    parse_dim_status_code,
    parse_dim_time,
)
from .warehouse.writer.dim_writer import ClickHouseDimWriter


def _build_spark() -> SparkSession:
    spark = SparkSession \
        .builder \
        .master("local[*]")  \
        .appName("LogForge_Dim_Batch") \
        .config("spark.jars.packages", "com.clickhouse:clickhouse-jdbc:0.4.6") \
        .config("spark.driver.bindAddress", "0.0.0.0") \
        .config("spark.ui.enabled", "true") \
        .config("spark.ui.port", "4041") \
        .getOrCreate()
    return spark


def _read_fact_log(spark: SparkSession):
    clickhouse_url = os.getenv(
        "SPARK_CLICKHOUSE_URL",
        "jdbc:clickhouse://clickhouse:8123/analytics?compress=0&decompress=0&jdbcCompliant=false",
    )
    clickhouse_user = os.getenv("SPARK_CLICKHOUSE_USER", "log_user")
    clickhouse_password = os.getenv("SPARK_CLICKHOUSE_PASSWORD", "log_pwd")
    lookback_days = int(os.getenv("DIM_BATCH_LOOKBACK_DAYS", "1"))

    # 최근 N일 데이터만 읽어서 dim을 갱신한다.
    query = f"""(
        SELECT event_ts, service, status_code
        FROM analytics.fact_log
        WHERE event_ts >= now() - INTERVAL {lookback_days} DAY
    ) AS fact"""

    return (
        spark.read
        .format("jdbc")
        .option("driver", "com.clickhouse.jdbc.ClickHouseDriver")
        .option("url", clickhouse_url)
        .option("user", clickhouse_user)
        .option("password", clickhouse_password)
        .option("dbtable", query)
        .load()
    )


def main() -> None:
    spark = _build_spark()
    spark.sparkContext.setLogLevel("INFO")

    fact_df = _read_fact_log(spark).cache()

    dim_date_df = parse_dim_date(fact_df)
    dim_time_df = parse_dim_time(fact_df)
    dim_service_df = parse_dim_service(fact_df)
    dim_status_df = parse_dim_status_code(fact_df)

    writer = ClickHouseDimWriter()
    writer.write_dim_date(dim_date_df)
    writer.write_dim_time(dim_time_df)
    writer.write_dim_service(dim_service_df)
    writer.write_dim_status(dim_status_df)

    spark.stop()


if __name__ == "__main__":
    main()
