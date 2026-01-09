# spark_job/fact/jobs/ingest_job.py
# spark-submit 진입점
# Kafka logs.* 토픽에서 데이터를 읽고 ClickHouse로 적재한다.

from __future__ import annotations

import os
import shutil
import time
from pyspark.sql import functions as F
from pyspark.sql.streaming import StreamingQueryException

from ...dlq import build_error_dlq_df, build_topic_dlq_df
from ...spark import build_streaming_spark
from ..normalization import normalize_event
from ..parsing import parse_event
from ..validation import validate_event
from ..writer import (
    ClickHouseStreamWriter,
    FACT_EVENT_CHECKPOINT_DIR,
)
from ...dlq.writer import ClickHouseDlqWriter

writer = ClickHouseStreamWriter()
dlq_writer = ClickHouseDlqWriter()


def _maybe_reset_checkpoint(checkpoint_dir: str) -> None:
    """
    Spark Structured Streaming 체크포인트가 깨졌을 때(예: 컨테이너 강제 종료),
    실시간 처리를 우선하는 모드에서는 체크포인트를 초기화하고 latest부터 다시 시작한다.
    """
    enabled = os.getenv("SPARK_RESET_CHECKPOINT_ON_START", "false").strip().lower() in (
        "1",
        "true",
        "yes",
        "y",
    )
    exists = os.path.exists(checkpoint_dir)
    if not enabled:
        if exists:
            print(
                f"[ℹ️ checkpoint] reset 비활성 (SPARK_RESET_CHECKPOINT_ON_START=false), 기존 사용: {checkpoint_dir}"
            )
        return
    if not exists:
        return

    ts = time.strftime("%Y%m%d-%H%M%S")
    backup = f"{checkpoint_dir}.bak.{ts}"
    print(f"[⚠️ checkpoint] reset 활성: 이동 {checkpoint_dir} -> {backup}")
    shutil.move(checkpoint_dir, backup)
    os.makedirs(checkpoint_dir, exist_ok=True)


def run_event_ingest() -> None:
    spark = None
    try:
        # 1) Spark 세션 생성
        spark = build_streaming_spark(master=os.getenv("SPARK_MASTER_URL"))
        spark.sparkContext.setLogLevel("INFO")

        # 체크포인트 손상 시(예: Incomplete log file) 실시간 모드에서 자동 초기화 옵션
        _maybe_reset_checkpoint(FACT_EVENT_CHECKPOINT_DIR)

        # 2) Kafka logs.* 토픽에서 스트리밍 데이터 읽기
        # 목표 처리량이 10k EPS라면, (배치 주기 dt) 기준으로 대략 maxOffsetsPerTrigger ~= 10000 * dt 로 잡아야 한다.
        max_offsets_per_trigger = os.getenv("SPARK_MAX_OFFSETS_PER_TRIGGER", "250000")
        starting_offsets = os.getenv("SPARK_STARTING_OFFSETS", "latest")  # "latest" | "earliest" | (토픽별 JSON)
        print(f"[ℹ️ spark] startingOffsets={starting_offsets} (체크포인트가 있으면 무시될 수 있음)")
        kafka_df = (
            spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", os.getenv("KAFKA_BOOTSTRAP"))
            .option("subscribePattern", "logs.*")
            .option("startingOffsets", starting_offsets)
            .option("failOnDataLoss", "false")
            .option("maxOffsetsPerTrigger", max_offsets_per_trigger)
            .load()
        )

        # 3) Kafka raw DF → fact_event 스키마로 파싱
        store_raw_json = os.getenv("SPARK_STORE_RAW_JSON", "false").strip().lower() in (
            "1",
            "true",
            "yes",
            "y",
        )
        event_source = kafka_df.where(F.col("topic") != F.lit("logs.dlq"))
        dlq_source = kafka_df.where(F.col("topic") == F.lit("logs.dlq"))

        parsed = parse_event(event_source)
        good_df, bad_df = validate_event(parsed)
        event_df = normalize_event(good_df, store_raw_json=store_raw_json)
        parse_error_dlq_df = build_error_dlq_df(bad_df)

        dlq_topics_df = build_topic_dlq_df(dlq_source)
        dlq_df = parse_error_dlq_df.unionByName(dlq_topics_df)

        # 4) ClickHouse analytics.fact_event로 스트리밍 적재
        writer.write_fact_event_stream(event_df)
        dlq_writer.write_dlq_stream(dlq_df)

        try:
            spark.streams.awaitAnyTermination()
        except StreamingQueryException as exc:
            # 드라이버 종료 원인 파악을 위해 전체 예외 메시지 출력
            print(f"[❌ 스트리밍 쿼리 예외] {exc}")
            raise

    except Exception as exc:
        print(f"[❌ SparkSession] 예기치 않은 오류: {exc}")
        raise

    finally:
        if spark:
            print("[ℹ️ SparkSession] 세션 종료.")
            spark.stop()
