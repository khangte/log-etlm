import os
import traceback

from .stats import record_clickhouse_write


def write_to_clickhouse(df, table_name, batch_id: int | None = None):
    batch_count: int | None = None
    min_ts = None
    max_ts = None
    write_succeeded = False
    cached = False

    try:
        log_stats = os.getenv("SPARK_CLICKHOUSE_LOG_STATS", "true").strip().lower() in ("1", "true", "yes", "y")
        log_stats_every = int(os.getenv("SPARK_CLICKHOUSE_LOG_STATS_EVERY", "1"))
        if log_stats_every < 1:
            log_stats_every = 1
        target_partitions = os.getenv("SPARK_CLICKHOUSE_WRITE_PARTITIONS")
        jdbc_batchsize = os.getenv("SPARK_CLICKHOUSE_JDBC_BATCHSIZE", "50000")
        clickhouse_url = os.getenv(
            "SPARK_CLICKHOUSE_URL",
            # async_insert=1 + wait_for_async_insert=0:
            # - Spark micro-batch에서 INSERT 응답 대기를 줄여 처리량을 올린다.
            # - 대시보드/분석은 MV 집계 테이블로 보므로, 약간의 지연은 허용 가능.
            "jdbc:clickhouse://clickhouse:8123/analytics?compress=0&decompress=0&jdbcCompliant=false&async_insert=1&wait_for_async_insert=0",
        )
        clickhouse_user = os.getenv("SPARK_CLICKHOUSE_USER", "log_user")
        clickhouse_password = os.getenv("SPARK_CLICKHOUSE_PASSWORD", "log_pwd")

        out_df = df
        if target_partitions and target_partitions.strip():
            n = int(target_partitions)
            current = out_df.rdd.getNumPartitions()
            if n < current:
                out_df = out_df.coalesce(n)
            elif n > current:
                out_df = out_df.repartition(n)

        # 고RPS에서는 배치마다 count/min/max를 구하면(추가 Spark job + 캐시/스필)
        # sink 자체 처리량이 크게 떨어질 수 있어 샘플링 옵션을 둔다.
        do_log_stats = log_stats and (log_stats_every == 1 or batch_id is None or (batch_id % log_stats_every) == 0)

        if do_log_stats:
            from pyspark.sql import functions as F
            out_df = out_df.persist()
            cached = True
            stats_row = out_df.agg(
                F.count(F.lit(1)).alias("rows"),
                F.min("event_ts").alias("min_ts"),
                F.max("event_ts").alias("max_ts"),
            ).collect()[0]
            batch_count = int(stats_row["rows"])
            min_ts = stats_row["min_ts"]
            max_ts = stats_row["max_ts"]

        if batch_count == 0 and do_log_stats:
            print(f"[📥 ClickHouse] Writing to {table_name} | rows=0")
            write_succeeded = True
            return

        if do_log_stats:
            print(f"[📥 ClickHouse] Writing to {table_name} | rows={batch_count} min_ts={min_ts} max_ts={max_ts}")

        writer = (
            out_df.write
            .format("jdbc") \
            .option("driver", "com.clickhouse.jdbc.ClickHouseDriver") \
            .option("url", clickhouse_url) \
            .option("user", clickhouse_user) \
            .option("password", clickhouse_password) \
            .option("dbtable", table_name) \
            .option("isolationLevel", "NONE") \
            .option("batchsize", jdbc_batchsize)
            .mode("append")
        )
        writer.save()
        write_succeeded = True
    except Exception as e:
        print(f"[❌ ERROR] Failed writing {table_name} to ClickHouse: {e}")
        msg = str(e)
        if "TABLE_ALREADY_EXISTS" in msg and "detached" in msg.lower():
            print(
                "[🛠️ ClickHouse] Table exists but is DETACHED. Fix by running:\n"
                "  sudo docker exec -it clickhouse clickhouse-client -u log_user --password log_pwd \\\n"
                "    --query \"ATTACH TABLE analytics.fact_log\""
            )
        traceback.print_exc()
    finally:
        try:
            if cached:
                out_df.unpersist()
        except Exception:
            pass
        record_clickhouse_write(write_succeeded, batch_count)


# --------------------------------------------------------------------------------
# "WARN JdbcUtils: Requested isolation level 1, but transactions are unsupported" 로그 무시
# 1. jdbcCompliant=false 옵션 추가
# 2. isolationLevel=NONE 옵션 추가
# --------------------------------------------------------------------------------
