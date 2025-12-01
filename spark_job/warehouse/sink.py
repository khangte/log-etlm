import traceback


def write_to_clickhouse(df, table_name):
    try:
        print(f"[📥 ClickHouse] Writing to {table_name}")
        df.show(5, truncate=False)

        df.write \
            .format("jdbc") \
            .option("driver", "com.clickhouse.jdbc.ClickHouseDriver") \
            .option("url", "jdbc:clickhouse://clickhouse:8123/analytics?compress=0&decompress=0&jdbcCompliant=false") \
            .option("user", "log_user") \
            .option("password", "log_pwd") \
            .option("dbtable", table_name) \
            .mode("append") \
            .save()
    except Exception as e:
        print(f"[❌ ERROR] Failed writing {table_name} to ClickHouse: {e}")
        traceback.print_exc()
