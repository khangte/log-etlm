from __future__ import annotations

import os


def read_fact_event(spark):
    """ClickHouse에서 최근 N일 fact_event를 읽어온다."""
    clickhouse_url = os.getenv(
        "SPARK_CLICKHOUSE_URL",
        "jdbc:clickhouse://clickhouse:8123/analytics?compress=0&decompress=0&jdbcCompliant=false",
    )
    clickhouse_user = os.getenv("SPARK_CLICKHOUSE_USER", "log_user")
    clickhouse_password = os.getenv("SPARK_CLICKHOUSE_PASSWORD", "log_pwd")
    lookback_days = int(os.getenv("DIM_BATCH_LOOKBACK_DAYS", "1"))

    query = f"""(
        SELECT event_ts, service, status_code, user_id
        FROM analytics.fact_event
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
