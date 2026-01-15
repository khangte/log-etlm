# spark_job/stream_ingest.py
# Kafka logs.* 토픽에서 데이터를 읽고 ClickHouse로 적재한다.

from __future__ import annotations

import os
import shutil
import time
from pyspark.sql import SparkSession

from .dlq.transforms.build_dlq_stream import build_dlq_stream_df
from .dlq.writers.kafka_writer import KafkaDlqWriter
from .fact.parsers.fact_event import parse_fact_event_with_errors
from .fact.writers.fact_writer import (
    ClickHouseFactWriter,
    FACT_EVENT_CHECKPOINT_DIR,
)
from .dlq.writers.dlq_writer import ClickHouseDlqWriter

writer = ClickHouseFactWriter()
dlq_writer = ClickHouseDlqWriter()
dlq_kafka_writer = KafkaDlqWriter()


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


def start_event_ingest_streams(spark: SparkSession) -> None:
    """fact/DLQ 스트림을 구성하고 실행한다."""
    # 체크포인트 손상 시(예: Incomplete log file) 실시간 모드에서 자동 초기화 옵션
    _maybe_reset_checkpoint(FACT_EVENT_CHECKPOINT_DIR)

    # Kafka logs.* 토픽에서 스트리밍 데이터 읽기
    # 목표 처리량이 10k EPS라면, (배치 주기 dt) 기준으로 대략 maxOffsetsPerTrigger ~= 10000 * dt 로 잡아야 한다.
    max_offsets_per_trigger = os.getenv("SPARK_MAX_OFFSETS_PER_TRIGGER", "250000")
    starting_offsets = os.getenv("SPARK_STARTING_OFFSETS", "latest")  # "latest" | "earliest" | (토픽별 JSON)
    print(f"[ℹ️ spark] startingOffsets={starting_offsets} (체크포인트가 있으면 무시될 수 있음)")
    fact_topics = os.getenv(
        "SPARK_FACT_TOPICS",
        "logs.auth,logs.order,logs.payment",
    )
    dlq_topic = os.getenv("SPARK_DLQ_TOPIC", "logs.dlq")
    enable_dlq_stream = os.getenv("SPARK_ENABLE_DLQ_STREAM", "true").strip().lower() in (
        "1",
        "true",
        "yes",
        "y",
    )

    def _build_kafka_stream(topics: str):
        """build_kafka_stream 처리를 수행한다."""
        return (
            spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", os.getenv("KAFKA_BOOTSTRAP"))
            .option("subscribe", topics)
            .option("startingOffsets", starting_offsets)
            .option("failOnDataLoss", "false")
            .option("maxOffsetsPerTrigger", max_offsets_per_trigger)
            .load()
        )

    event_kafka_df = _build_kafka_stream(fact_topics)

    # Kafka raw DF → fact_event 스키마로 파싱
    event_source = event_kafka_df

    event_df, bad_df = parse_fact_event_with_errors(event_source)

    # ClickHouse analytics.fact_event로 스트리밍 적재
    writer.write_fact_event_stream(event_df)
    if enable_dlq_stream:
        dlq_kafka_writer.write_dlq_kafka_stream(bad_df, topic=dlq_topic)
        dlq_source = _build_kafka_stream(dlq_topic)
        dlq_df = build_dlq_stream_df(dlq_source)
        dlq_writer.write_dlq_stream(dlq_df)
    else:
        print("[ℹ️ spark] DLQ 스트림 비활성화: bad_df는 저장하지 않음")
