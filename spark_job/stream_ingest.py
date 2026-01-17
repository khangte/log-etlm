# 파일명 : spark_job/stream_ingest.py
# 목적   : Kafka logs.* 토픽에서 데이터를 읽고 ClickHouse로 적재한다.

from __future__ import annotations

import os
import shutil
import time
from dataclasses import dataclass

from pyspark.sql import DataFrame, SparkSession

from .dlq.settings import (
    DlqKafkaSettings,
    DlqStreamSettings,
    get_dlq_kafka_settings,
    get_dlq_stream_settings,
)
from .dlq.transforms.build_dlq_stream import build_dlq_stream_df
from .dlq.writers.dlq_writer import ClickHouseDlqWriter
from .dlq.writers.kafka_writer import KafkaDlqWriter
from .fact.parsers.fact_event import parse_fact_event_with_errors
from .fact.settings import FactStreamSettings, get_fact_stream_settings
from .fact.writers.fact_writer import ClickHouseFactWriter
from .stream_settings import StreamIngestSettings, get_stream_ingest_settings


@dataclass(frozen=True)
class StreamIngestJob:
    """스트리밍 적재 작업을 구성한다."""

    settings: StreamIngestSettings
    fact_settings: FactStreamSettings
    fact_writer: ClickHouseFactWriter
    dlq_writer: ClickHouseDlqWriter
    dlq_kafka_writer: KafkaDlqWriter

    def run(self, spark: SparkSession) -> None:
        """fact/DLQ 스트림을 구성하고 실행한다."""
        self._maybe_reset_checkpoint(self.fact_settings.checkpoint_dir)

        event_kafka_df = self._build_kafka_stream(
            spark,
            self.settings.fact_topics,
        )

        event_df, bad_df = parse_fact_event_with_errors(
            event_kafka_df,
            store_raw_json=self.fact_settings.store_raw_json,
        )

        self.fact_writer.write_fact_event_stream(event_df)
        if self.settings.enable_dlq_stream:
            self._run_dlq_streams(spark, bad_df)
        else:
            print("[ℹ️ spark] DLQ 스트림 비활성화: bad_df는 저장하지 않음")

    def _run_dlq_streams(self, spark: SparkSession, bad_df: DataFrame) -> None:
        """DLQ 스트림을 구성해 실행한다."""
        dlq_topic = self._require_dlq_topic()
        self.dlq_kafka_writer.write_dlq_kafka_stream(bad_df, topic=dlq_topic)
        dlq_source = self._build_kafka_stream(spark, dlq_topic)
        dlq_df = build_dlq_stream_df(dlq_source)
        self.dlq_writer.write_dlq_stream(dlq_df)

    def _require_dlq_topic(self) -> str:
        """DLQ 토픽 설정을 검증한다."""
        dlq_topic = (self.settings.dlq_topic or "").strip()
        if not dlq_topic:
            raise ValueError("SPARK_DLQ_TOPIC is required when DLQ stream is enabled")
        return dlq_topic

    def _build_kafka_stream(self, spark: SparkSession, topics: str) -> DataFrame:
        """Kafka 스트림 데이터프레임을 생성한다."""
        topics = (topics or "").strip()
        if not topics:
            raise ValueError("SPARK_FACT_TOPICS is required")
        if not self.settings.kafka_bootstrap:
            raise ValueError("KAFKA_BOOTSTRAP is required")

        reader = (
            spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", self.settings.kafka_bootstrap)
            .option("subscribe", topics)
            .option("failOnDataLoss", "false")
        )
        if self.settings.starting_offsets:
            print(
                "[ℹ️ spark] "
                f"startingOffsets={self.settings.starting_offsets} "
                "(체크포인트가 있으면 무시될 수 있음)"
            )
            reader = reader.option("startingOffsets", self.settings.starting_offsets)
        if self.settings.max_offsets_per_trigger:
            reader = reader.option(
                "maxOffsetsPerTrigger",
                self.settings.max_offsets_per_trigger,
            )
        return reader.load()

    def _maybe_reset_checkpoint(self, checkpoint_dir: str) -> None:
        """체크포인트 초기화 여부를 처리한다."""
        exists = os.path.exists(checkpoint_dir)
        if not self.settings.reset_checkpoint_on_start:
            if exists:
                print(
                    "[ℹ️ checkpoint] reset 비활성 "
                    f"(SPARK_RESET_CHECKPOINT_ON_START=false), 기존 사용: {checkpoint_dir}"
                )
            return
        if not exists:
            return

        ts = time.strftime("%Y%m%d-%H%M%S")
        backup = f"{checkpoint_dir}.bak.{ts}"
        print(f"[⚠️ checkpoint] reset 활성: 이동 {checkpoint_dir} -> {backup}")
        shutil.move(checkpoint_dir, backup)
        os.makedirs(checkpoint_dir, exist_ok=True)


def build_stream_ingest_job(
    *,
    settings: StreamIngestSettings | None = None,
    fact_settings: FactStreamSettings | None = None,
    dlq_stream_settings: DlqStreamSettings | None = None,
    dlq_kafka_settings: DlqKafkaSettings | None = None,
    fact_writer: ClickHouseFactWriter | None = None,
    dlq_writer: ClickHouseDlqWriter | None = None,
    dlq_kafka_writer: KafkaDlqWriter | None = None,
) -> StreamIngestJob:
    """스트리밍 적재 작업을 생성한다."""
    resolved_settings = settings or get_stream_ingest_settings()
    resolved_fact_settings = fact_settings or get_fact_stream_settings()
    resolved_dlq_stream_settings = dlq_stream_settings or get_dlq_stream_settings()
    resolved_dlq_kafka_settings = dlq_kafka_settings or get_dlq_kafka_settings()

    return StreamIngestJob(
        settings=resolved_settings,
        fact_settings=resolved_fact_settings,
        fact_writer=fact_writer or ClickHouseFactWriter(resolved_fact_settings),
        dlq_writer=dlq_writer or ClickHouseDlqWriter(resolved_dlq_stream_settings),
        dlq_kafka_writer=dlq_kafka_writer or KafkaDlqWriter(resolved_dlq_kafka_settings),
    )


def start_event_ingest_streams(spark: SparkSession) -> None:
    """스트리밍 적재 작업을 시작한다."""
    job = build_stream_ingest_job()
    job.run(spark)
