import traceback


def write_to_clickhouse(df, table_name):
    try:
        batch_count = df.count()
        min_ts = df.agg({"event_ts": "min"}).collect()[0][0] if batch_count else None
        max_ts = df.agg({"event_ts": "max"}).collect()[0][0] if batch_count else None
        print(f"[📥 ClickHouse] Writing to {table_name} | rows={batch_count} min_ts={min_ts} max_ts={max_ts}")
        df.write \
            .format("jdbc") \
            .option("driver", "com.clickhouse.jdbc.ClickHouseDriver") \
            .option("url", "jdbc:clickhouse://clickhouse:8123/analytics?compress=0&decompress=0&jdbcCompliant=false") \
            .option("user", "log_user") \
            .option("password", "log_pwd") \
            .option("dbtable", table_name) \
            .option("isolationLevel", "NONE") \
            .mode("append") \
            .save()
    except Exception as e:
        print(f"[❌ ERROR] Failed writing {table_name} to ClickHouse: {e}")
        traceback.print_exc()


# --------------------------------------------------------------------------------
# "WARN JdbcUtils: Requested isolation level 1, but transactions are unsupported" 로그 무시
# 1. jdbcCompliant=false 옵션 추가
# 2. isolationLevel=NONE 옵션 추가
# --------------------------------------------------------------------------------
