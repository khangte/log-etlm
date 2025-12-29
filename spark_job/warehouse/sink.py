import os
import traceback


def write_to_clickhouse(
    df,
    table_name,
    batch_id: int | None = None,
    mode: str = "append",
):
    cached = False

    try:
        target_partitions = os.getenv("SPARK_CLICKHOUSE_WRITE_PARTITIONS")
        jdbc_batchsize = os.getenv("SPARK_CLICKHOUSE_JDBC_BATCHSIZE", "50000")
        clickhouse_url = os.getenv(
            "SPARK_CLICKHOUSE_URL",
            "jdbc:clickhouse://clickhouse:8123/analytics?compress=0&decompress=0&jdbcCompliant=false&async_insert=1&wait_for_async_insert=0",
            # async_insert=1 + wait_for_async_insert=0:
            # - Spark micro-batch에서 INSERT 응답 대기를 줄여 처리량을 올린다.
            # - 대시보드/분석은 MV 집계 테이블로 보므로, 약간의 지연은 허용 가능.
        )
        clickhouse_user = os.getenv("SPARK_CLICKHOUSE_USER", "log_user")
        clickhouse_password = os.getenv("SPARK_CLICKHOUSE_PASSWORD", "log_pwd")

        out_df = df
        if target_partitions and target_partitions.strip():
            n = int(target_partitions)
            current = out_df.rdd.getNumPartitions()
            if n < current:
                # 셔플 없이 파티션 수를 줄여 쓰기 오버헤드를 낮춘다.
                out_df = out_df.coalesce(n)
            elif n > current:
                # 병렬 쓰기를 늘리기 위해 파티션을 재분배한다.
                out_df = out_df.repartition(n)

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
            .mode(mode)
        )
        writer.save()

    except Exception as e:
        print(f"[❌ ERROR] ClickHouse 저장 실패: {table_name} {e}")
        msg = str(e)
        if "TABLE_ALREADY_EXISTS" in msg and "detached" in msg.lower():
            print(
                "[🛠️ ClickHouse] 테이블이 DETACHED 상태입니다. 아래 명령으로 복구하세요:\n"
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
