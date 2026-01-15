# spark_job/jobs/dim_batch_job.py
# 하루 1회 배치로 Dimension 테이블을 갱신한다.
# NOTE: 이 파일은 자동 실행되지 않으며, 수동 또는 크론으로 실행해야 한다.

from __future__ import annotations

import os

from ..parsers import (
    parse_dim_date,
    parse_dim_service,
    parse_dim_status_code,
    parse_dim_time,
    parse_dim_user,
)
from ...spark import build_batch_spark
from ..writers.dim_writer import ClickHouseDimWriter


def _read_fact_event(spark):
    """read_fact_event 처리를 수행한다."""
    clickhouse_url = os.getenv(
        "SPARK_CLICKHOUSE_URL",
        "jdbc:clickhouse://clickhouse:8123/analytics?compress=0&decompress=0&jdbcCompliant=false",
    )
    clickhouse_user = os.getenv("SPARK_CLICKHOUSE_USER", "log_user")
    clickhouse_password = os.getenv("SPARK_CLICKHOUSE_PASSWORD", "log_pwd")
    lookback_days = int(os.getenv("DIM_BATCH_LOOKBACK_DAYS", "1"))

    # 최근 N일 데이터만 읽어서 dim을 갱신한다.
    query = f"""(
        SELECT ingest_ts, event_ts, service, status_code, user_id
        FROM analytics.fact_event
        WHERE ingest_ts >= now() - INTERVAL {lookback_days} DAY
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


def _read_service_map(spark):
    """외부 서비스 메타 매핑 파일을 읽는다."""
    path = os.getenv("DIM_SERVICE_MAP_PATH", "").strip()
    if not path:
        return None
    return (
        spark.read
        .option("header", "true")
        .option("inferSchema", "false")
        .csv(path)
        .select("service", "service_group", "is_active", "description")
    )


def run_dim_batch() -> None:
    """run_dim_batch 처리를 수행한다."""
    spark = build_batch_spark()
    spark.sparkContext.setLogLevel("INFO")

    fact_df = _read_fact_event(spark)
    service_map_df = _read_service_map(spark)

    dim_date_df = parse_dim_date(fact_df, time_col="ingest_ts")
    dim_time_df = parse_dim_time(fact_df, time_col="ingest_ts")
    dim_service_df = parse_dim_service(fact_df, service_map_df=service_map_df)
    dim_status_df = parse_dim_status_code(fact_df)
    dim_user_df = parse_dim_user(fact_df)

    writer = ClickHouseDimWriter()
    writer.write_dim_date(dim_date_df)
    writer.write_dim_time(dim_time_df)
    writer.write_dim_service(dim_service_df)
    writer.write_dim_status(dim_status_df)
    writer.write_dim_user(dim_user_df)

    spark.stop()


if __name__ == "__main__":
    run_dim_batch()
