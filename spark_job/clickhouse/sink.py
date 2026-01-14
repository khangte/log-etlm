import os
import traceback


def _apply_partitioning(df, target_partitions: str | None):
    """apply_partitioning 처리를 수행한다."""
    if not target_partitions or not target_partitions.strip():
        return df
    try:
        n = int(target_partitions)
    except ValueError:
        return df

    allow_repartition = os.getenv("SPARK_CLICKHOUSE_ALLOW_REPARTITION", "false").strip().lower() in (
        "1",
        "true",
        "yes",
        "y",
    )
    current = df.rdd.getNumPartitions()
    if n < current:
        # 셔플 없이 파티션 수를 줄여 쓰기 오버헤드를 낮춘다.
        return df.coalesce(n)
    if n > current:
        if allow_repartition:
            # 병렬 쓰기를 늘리기 위해 파티션을 재분배한다.
            return df.repartition(n)
        print(
            "[ℹ️ clickhouse sink] repartition 비활성: "
            "SPARK_CLICKHOUSE_ALLOW_REPARTITION=true로 켜세요."
        )
    return df


def write_to_clickhouse(
    df,
    table_name,
    batch_id: int | None = None,
    mode: str = "append",
):
    """write_to_clickhouse 처리를 수행한다."""
    cached = False

    try:
        target_partitions = os.getenv("SPARK_CLICKHOUSE_WRITE_PARTITIONS")
        jdbc_batchsize = os.getenv("SPARK_CLICKHOUSE_JDBC_BATCHSIZE", "50000")
        clickhouse_url = os.getenv(
            "SPARK_CLICKHOUSE_URL",
            "jdbc:clickhouse://clickhouse:8123/analytics?compress=0&decompress=0&jdbcCompliant=false",
        )
        # async_insert=1이 이 클러스터에서 ClickHouse "Unknown error 1002"를 발생시켜서,
        # 비동기 쓰기가 명시적으로 지원될 때까지 동기 파이프라인을 유지합니다.
        clickhouse_user = os.getenv("SPARK_CLICKHOUSE_USER", "log_user")
        clickhouse_password = os.getenv("SPARK_CLICKHOUSE_PASSWORD", "log_pwd")

        out_df = _apply_partitioning(df, target_partitions)

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
                f"    --query \"ATTACH TABLE {table_name}\""
            )
        traceback.print_exc()
    finally:
        try:
            if cached:
                out_df.unpersist()
        except Exception:
            pass
