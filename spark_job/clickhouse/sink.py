import traceback

from .settings import ClickHouseSettings, get_clickhouse_settings


def _apply_partitioning(
    df,
    *,
    target_partitions: int | None,
    allow_repartition: bool,
):
    """파티션 수를 조정한다."""
    if target_partitions is None or target_partitions <= 0:
        return df

    n = target_partitions
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
    *,
    settings: ClickHouseSettings | None = None,
):
    """ClickHouse로 데이터를 적재한다."""
    try:
        resolved_settings = settings or get_clickhouse_settings()

        out_df = _apply_partitioning(
            df,
            target_partitions=resolved_settings.write_partitions,
            allow_repartition=resolved_settings.allow_repartition,
        )

        writer = out_df.write.format("jdbc")
        for key, value in resolved_settings.build_jdbc_options(table_name).items():
            writer = writer.option(key, value)
        writer = writer.mode(mode)
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
