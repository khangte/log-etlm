import time
import traceback

from pyspark.sql import DataFrame, functions as F

from ..dlq.schema import DLQ_VALUE_COLUMNS
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
    print(
        "[clickhouse sink] 파티션 조정 target=%s current=%s allow_repartition=%s"
        % (n, current, allow_repartition)
    )

    if n < current:
        return df.coalesce(n)  # 다운스케일은 셔플 없이 coalesce로 줄여 초소형 파티션 생성을 완화한다.

    if n > current and allow_repartition:
        return df.repartition(n)
    return df


def _dlq_project(df: DataFrame, col_name: str, cast_type: str):
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
    """fact_event 쓰기 실패 배치를 DLQ payload 스키마로 변환한다."""
    trimmed_message = (error_message or "")[:2000]
    created_ms = (
        F.when(F.col("kafka_ts").isNotNull(), (F.col("kafka_ts").cast("double") * F.lit(1000)).cast("long"))
        .otherwise((F.current_timestamp().cast("double") * F.lit(1000)).cast("long"))
        if "kafka_ts" in df.columns
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
    """최종 실패한 fact_event 배치를 DLQ Kafka로 우회 적재한다."""
    if not settings.dlq_on_final_failure:
        return False
    if table_name != "analytics.fact_event":
        return False
    if not settings.dlq_topic or not settings.kafka_bootstrap:
        print(
            f"[⚠️ DLQ bypass skipped] table={table_name} batch_id={batch_id} "
            "reason=missing SPARK_DLQ_TOPIC or KAFKA_BOOTSTRAP"
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

    print(
        f"[⚠️ DLQ bypass] table={table_name} batch_id={batch_id} "
        f"topic={settings.dlq_topic} reason=clickhouse_final_failure"
    )
    return True


def write_to_clickhouse(
    df,
    table_name,
    batch_id: int | None = None,
    mode: str = "append",
    *,
    settings: ClickHouseSettings | None = None,
):
    """ClickHouse로 데이터를 적재한다."""
    resolved_settings = settings or get_clickhouse_settings()
    max_attempts = max(1, resolved_settings.retry_max + 1)
    backoff_sec = max(0.0, resolved_settings.retry_backoff_sec)

    for attempt in range(1, max_attempts + 1):
        out_df = df
        try:
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
            return

        except Exception as e:
            batch_info = f" batch_id={batch_id}" if batch_id is not None else ""
            print(
                f"[❌ ERROR] ClickHouse 저장 실패: {table_name}{batch_info} "
                f"(attempt {attempt}/{max_attempts}) {e}"
            )
            if attempt < max_attempts:
                if backoff_sec > 0:
                    time.sleep(backoff_sec * attempt)
                continue

            msg = str(e)
            if "TABLE_ALREADY_EXISTS" in msg and "detached" in msg.lower():
                print(
                    "[🛠️ ClickHouse] 테이블이 DETACHED 상태입니다. 아래 명령으로 복구하세요:\n"
                    "  sudo docker exec -it clickhouse clickhouse-client -u log_user --password log_pwd \\\n"
                    f"    --query \"ATTACH TABLE {table_name}\""
                )
            traceback.print_exc()
            try:
                bypassed = _write_failed_fact_batch_to_dlq(
                    out_df,
                    settings=resolved_settings,
                    table_name=table_name,
                    batch_id=batch_id,
                    error=e,
                )
            except Exception as dlq_error:
                print(
                    f"[❌ ERROR] DLQ 우회 적재 실패: {table_name}{batch_info} {dlq_error}"
                )
                traceback.print_exc()
                bypassed = False

            if bypassed:
                return
            if resolved_settings.fail_on_error:
                raise
            return
