import logging
import re

from pyspark.sql import DataFrame, functions as F

from ..dlq.schema import DLQ_VALUE_COLUMNS
from .settings import ClickHouseSettings, get_clickhouse_settings

logger = logging.getLogger(__name__)

_TABLE_IDENTIFIER_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_.]*$")
_guard_table_missing_warned = False


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


def _dlq_project(df: DataFrame, col_name: str, cast_type: str) -> F.Column:
    """컬럼 존재 여부에 따라 DLQ 컬럼을 안전하게 구성한다."""
    if col_name in df.columns:
        return F.col(col_name).cast(cast_type)
    return F.lit(None).cast(cast_type)


def _build_failed_fact_dlq_payload(
    df: DataFrame,
    *,
    error_type: str,
    error_message: str,
) -> DataFrame:
    """event_log 쓰기 실패 배치를 DLQ payload 스키마로 변환한다."""
    trimmed_message = (error_message or "")[:2000]
    created_ms = (
        F.when(F.col("kafka_received_at").isNotNull(), (F.col("kafka_received_at").cast("double") * F.lit(1000)).cast("long"))
        .otherwise((F.current_timestamp().cast("double") * F.lit(1000)).cast("long"))
        if "kafka_received_at" in df.columns
        else (F.current_timestamp().cast("double") * F.lit(1000)).cast("long")
    )
    raw_json = (
        _dlq_project(df, "raw_json", "string")
        if "raw_json" in df.columns
        else F.to_json(F.struct(*[F.col(name) for name in df.columns]))
    )

    payload_df = df.select(
        F.lit(error_type).alias("error_type"),
        F.lit(trimmed_message).alias("error_message"),
        _dlq_project(df, "service", "string").alias("service"),
        _dlq_project(df, "event_id", "string").alias("event_id"),
        _dlq_project(df, "request_id", "string").alias("request_id"),
        _dlq_project(df, "topic", "string").alias("source_topic"),
        _dlq_project(df, "kafka_partition", "int").alias("source_partition"),
        _dlq_project(df, "kafka_offset", "long").alias("source_offset"),
        F.coalesce(
            _dlq_project(df, "source_key", "string"),
            _dlq_project(df, "event_id", "string"),
        ).alias("source_key"),
        created_ms.alias("created_ms"),
        raw_json.alias("raw_json"),
    )
    return payload_df.select(*DLQ_VALUE_COLUMNS)


def _write_failed_fact_batch_to_dlq(
    df: DataFrame,
    *,
    settings: ClickHouseSettings,
    table_name: str,
    batch_id: int | None,
    error: Exception,
) -> bool:
    """최종 실패한 event_log 배치를 DLQ Kafka로 우회 적재한다."""
    if not settings.dlq_on_final_failure:
        return False
    if table_name != "analytics.event_log":
        return False
    if not settings.dlq_topic or not settings.kafka_bootstrap:
        logger.warning(
            "[WARN] DLQ bypass skipped table=%s batch_id=%s "
            "reason=missing SPARK_DLQ_TOPIC or KAFKA_BOOTSTRAP",
            table_name, batch_id,
        )
        return False

    dlq_payload = _build_failed_fact_dlq_payload(
        df,
        error_type="clickhouse_write_failed",
        error_message=str(error),
    )
    value_struct = F.struct(*[F.col(name) for name in DLQ_VALUE_COLUMNS])
    kafka_df = dlq_payload.select(
        F.coalesce(F.col("source_key"), F.col("event_id")).cast("string").alias("key"),
        F.to_json(value_struct).alias("value"),
    )

    kafka_df.write.format("kafka").option(
        "kafka.bootstrap.servers",
        settings.kafka_bootstrap,
    ).option(
        "topic",
        settings.dlq_topic,
    ).save()

    logger.info(
        "[INFO] DLQ bypass table=%s batch_id=%s topic=%s reason=clickhouse_final_failure",
        table_name, batch_id, settings.dlq_topic,
    )
    return True


def _ensure_table_identifier(name: str) -> str:
    """JDBC query에 사용할 테이블 식별자 안전성을 검증한다."""
    if not _TABLE_IDENTIFIER_PATTERN.fullmatch(name):
        raise ValueError(f"invalid table identifier: {name}")
    return name


def _escape_sql_literal(value: str) -> str:
    """SQL literal 문자열을 이스케이프한다."""
    return (value or "").replace("\\", "\\\\").replace("'", "''")


def _load_single_count(
    df: DataFrame,
    *,
    settings: ClickHouseSettings,
    query: str,
) -> int:
    """JDBC 단건 count 조회를 수행한다."""
    reader = (
        df.sparkSession.read.format("jdbc")
        .option("driver", "com.clickhouse.jdbc.ClickHouseDriver")
        .option("url", settings.url)
        .option("dbtable", f"({query}) AS t")
    )
    if settings.user:
        reader = reader.option("user", settings.user)
    if settings.password:
        reader = reader.option("password", settings.password)
    row = reader.load().first()
    if not row:
        return 0
    return int(row["cnt"] or 0)


def _is_already_committed_batch(
    df: DataFrame,
    *,
    settings: ClickHouseSettings,
    stream_name: str,
    table_name: str,
    batch_id: int,
) -> bool:
    """배치 커밋 가드 테이블에서 중복 배치 여부를 확인한다."""
    guard_table = _ensure_table_identifier(settings.batch_guard_table)
    escaped_stream = _escape_sql_literal(stream_name)
    escaped_table = _escape_sql_literal(table_name)
    query = (
        "SELECT toInt64(count()) AS cnt "
        f"FROM {guard_table} "
        f"WHERE stream_name = '{escaped_stream}' "
        f"AND target_table = '{escaped_table}' "
        f"AND batch_id = {int(batch_id)}"
    )
    return _load_single_count(df, settings=settings, query=query) > 0


def _mark_batch_committed(
    df: DataFrame,
    *,
    settings: ClickHouseSettings,
    stream_name: str,
    table_name: str,
    batch_id: int,
) -> None:
    """배치 커밋 가드 테이블에 성공 배치를 기록한다."""
    marker_df = df.sparkSession.createDataFrame(
        [(stream_name, table_name, int(batch_id))],
        ["stream_name", "target_table", "batch_id"],
    )
    writer = marker_df.write.format("jdbc")
    for key, value in settings.build_jdbc_options(settings.batch_guard_table).items():
        writer = writer.option(key, value)
    writer.mode("append").save()


def _is_missing_guard_table_error(error: Exception, guard_table: str) -> bool:
    """가드 테이블 미존재 오류인지 판별한다."""
    msg = str(error).lower()
    table = (guard_table or "").lower()
    if table and table not in msg:
        return False
    return ("unknown table" in msg) or ("doesn't exist" in msg)


def write_to_clickhouse(
    df: DataFrame,
    table_name: str,
    batch_id: int | None = None,
    stream_name: str | None = None,
    mode: str = "append",
    *,
    current_partitions: int | None = None,
    settings: ClickHouseSettings | None = None,
) -> None:
    """ClickHouse로 데이터를 적재한다."""
    resolved_settings = settings or get_clickhouse_settings()
    max_attempts = max(1, resolved_settings.retry_max + 1)
    use_batch_guard = bool(
        resolved_settings.batch_guard_enabled
        and batch_id is not None
        and stream_name
    )

    if use_batch_guard:
        try:
            if _is_already_committed_batch(
                df,
                settings=resolved_settings,
                stream_name=stream_name or "",
                table_name=table_name,
                batch_id=int(batch_id),
            ):
                logger.info(
                    "[clickhouse sink] 중복 배치 skip stream=%s table=%s batch_id=%s",
                    stream_name, table_name, batch_id,
                )
                return
        except Exception as guard_error:
            if _is_missing_guard_table_error(
                guard_error,
                resolved_settings.batch_guard_table,
            ):
                global _guard_table_missing_warned
                if not _guard_table_missing_warned:
                    logger.warning(
                        "[WARN] clickhouse sink batch guard 비활성화: "
                        "guard table missing (%s)",
                        resolved_settings.batch_guard_table,
                    )
                    _guard_table_missing_warned = True
                use_batch_guard = False
            else:
                raise

    for attempt in range(1, max_attempts + 1):
        out_df = df
        data_written = False
        try:
            out_df = _apply_partitioning(
                df,
                target_partitions=resolved_settings.write_partitions,
                allow_repartition=resolved_settings.allow_repartition,
                current_partitions=current_partitions,
            )

            writer = out_df.write.format("jdbc")
            for key, value in resolved_settings.build_jdbc_options(table_name).items():
                writer = writer.option(key, value)
            writer = writer.mode(mode)
            writer.save()
            data_written = True

            if use_batch_guard:
                _mark_batch_committed(
                    out_df,
                    settings=resolved_settings,
                    stream_name=stream_name or "",
                    table_name=table_name,
                    batch_id=int(batch_id),
                )
            return

        except Exception as e:
            batch_info = f" batch_id={batch_id}" if batch_id is not None else ""
            if data_written and use_batch_guard:
                logger.error(
                    "[ERROR] 배치 가드 기록 실패: %s%s 데이터는 이미 저장됨(자동 재시도 중단)",
                    table_name, batch_info,
                )
                if resolved_settings.fail_on_error:
                    raise
                return

            logger.error(
                "[ERROR] ClickHouse 저장 실패: %s%s (attempt %s/%s) %s",
                table_name, batch_info, attempt, max_attempts, e,
            )
            if attempt < max_attempts:
                # foreachBatch는 driver 스레드에서 실행되므로 sleep 없이 즉시 재시도한다.
                # sleep이 필요한 경우 예외를 올려 Spark 체크포인트 기반 재처리를 활용한다.
                continue

            msg = str(e)
            if "TABLE_ALREADY_EXISTS" in msg and "detached" in msg.lower():
                logger.error(
                    "[ERROR] ClickHouse 테이블이 DETACHED 상태입니다. 아래 명령으로 복구하세요:\n"
                    "  sudo docker exec -it clickhouse clickhouse-client -u log_user --password $CLICKHOUSE_PASSWORD \\\n"
                    "    --query \"ATTACH TABLE %s\"",
                    table_name,
                )
            logger.exception("[ERROR] ClickHouse 저장 최종 실패: %s%s", table_name, batch_info)
            try:
                bypassed = _write_failed_fact_batch_to_dlq(
                    out_df,
                    settings=resolved_settings,
                    table_name=table_name,
                    batch_id=batch_id,
                    error=e,
                )
            except Exception as dlq_error:
                logger.exception(
                    "[ERROR] DLQ 우회 적재 실패: %s%s %s",
                    table_name, batch_info, dlq_error,
                )
                bypassed = False

            if bypassed:
                return
            if resolved_settings.fail_on_error:
                raise
            return
