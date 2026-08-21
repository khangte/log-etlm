import logging

from pyspark.sql import DataFrame

from .settings import ClickHouseSettings, get_clickhouse_settings

logger = logging.getLogger(__name__)


def _apply_partitioning(
    df: DataFrame,
    *,
    target_partitions: int | None,
    allow_repartition: bool,
    current_partitions: int | None = None,
) -> DataFrame:
    """파티션 수를 조정한다."""
    if target_partitions is None or target_partitions <= 0:
        return df

    n = target_partitions
    current = current_partitions if current_partitions is not None else df.rdd.getNumPartitions()
    logger.info(
        "[clickhouse sink] 파티션 조정 target=%s current=%s allow_repartition=%s",
        n, current, allow_repartition,
    )

    if n < current:
        return df.coalesce(n)  # 다운스케일은 셔플 없이 coalesce로 줄여 초소형 파티션 생성을 완화한다.

    if n > current and allow_repartition:
        return df.repartition(n)
    return df


def write_to_clickhouse(
    df: DataFrame,
    table_name: str,
    mode: str = "append",
    *,
    current_partitions: int | None = None,
    settings: ClickHouseSettings | None = None,
) -> None:
    """ClickHouse로 데이터를 적재한다. 실패 시 예외를 그대로 올려 Spark 체크포인트 기반 재처리에 위임한다."""
    resolved_settings = settings or get_clickhouse_settings()
    out_df = _apply_partitioning(
        df,
        target_partitions=resolved_settings.write_partitions,
        allow_repartition=resolved_settings.allow_repartition,
        current_partitions=current_partitions,
    )

    writer = out_df.write.format("jdbc")
    for key, value in resolved_settings.build_jdbc_options(table_name).items():
        writer = writer.option(key, value)
    writer.mode(mode).save()
