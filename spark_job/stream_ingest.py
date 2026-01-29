# 파일명 : spark_job/stream_ingest.py
# 목적   : Kafka logs.* 토픽에서 데이터를 읽고 ClickHouse로 적재한다.

from __future__ import annotations

import os
import shutil
import time
from dataclasses import dataclass
import re

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
            build_bad_df=self.settings.enable_dlq_stream,
        )

        self.fact_writer.write_fact_event_stream(event_df)
        if self.settings.enable_dlq_stream:
            if bad_df is None:
                raise ValueError("DLQ stream enabled but bad_df is missing")
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
        max_offsets = self.settings.max_offsets_per_trigger
        if not max_offsets:
            auto_offsets = self._compute_max_offsets_per_trigger()
            if auto_offsets:
                max_offsets = str(auto_offsets)
                print(f"[ℹ️ spark] maxOffsetsPerTrigger(auto)={max_offsets}")
        if max_offsets:
            reader = reader.option("maxOffsetsPerTrigger", max_offsets)

        min_partitions = self._resolve_min_partitions(spark, topics)
        if min_partitions:
            reader = reader.option("minPartitions", str(min_partitions))
        return reader.load()

    def _resolve_min_partitions(self, spark: SparkSession, topics: str) -> int | None:
        """스큐 완화를 위해 minPartitions를 계산/반환한다."""
        if self.settings.kafka_min_partitions:
            return self.settings.kafka_min_partitions
        multiplier = self.settings.kafka_min_partitions_multiplier
        if not multiplier:
            return None
        partition_count = self._fetch_kafka_partition_count(spark, topics)
        if not partition_count:
            return None
        return max(1, int(partition_count * multiplier))
        return None

    def _fetch_kafka_partition_count(
        self, spark: SparkSession, topics: str
    ) -> int | None:
        """Kafka 메타데이터에서 파티션 수를 조회한다."""
        topic_list = [t.strip() for t in (topics or "").split(",") if t.strip()]
        if not topic_list:
            return None
        try:
            jvm = spark._jvm
            props = jvm.java.util.Properties()
            props.put("bootstrap.servers", self.settings.kafka_bootstrap)
            admin = jvm.org.apache.kafka.clients.admin.AdminClient.create(props)
            try:
                java_topics = jvm.java.util.ArrayList()
                for topic in topic_list:
                    java_topics.add(topic)
                desc = admin.describeTopics(java_topics).all().get()
                it = desc.entrySet().iterator()
                total = 0
                while it.hasNext():
                    entry = it.next()
                    total += entry.getValue().partitions().size()
                return int(total)
            finally:
                admin.close()
        except Exception as exc:
            print(f"[⚠️ spark] Kafka 파티션 수 조회 실패: {exc}")
            return None

    def _compute_max_offsets_per_trigger(self) -> int | None:
        """목표 EPS와 트리거 간격으로 maxOffsetsPerTrigger를 계산한다."""
        target_eps = self._resolve_target_eps()
        if not target_eps:
            return None
        trigger_seconds = self._parse_duration_seconds(
            self.fact_settings.trigger_interval
        )
        if not trigger_seconds:
            return None
        safety = self.settings.max_offsets_safety or 1.0
        computed = int(target_eps * trigger_seconds * safety)
        return max(1, computed)

    def _resolve_target_eps(self) -> int | None:
        """target_eps를 환경/프로파일에서 우선순위로 결정한다."""
        if self.settings.target_eps:
            return self.settings.target_eps
        profile_path = self._resolve_target_eps_profile_path()
        if not profile_path:
            return None
        return self._read_eps_from_profile(profile_path)

    def _resolve_target_eps_profile_path(self) -> str | None:
        """profiles.yml 경로를 결정한다."""
        if self.settings.target_eps_profile_path:
            return self.settings.target_eps_profile_path
        default_path = "/app/log_simulator/config/profiles.yml"
        if os.path.exists(default_path):
            return default_path
        return None

    @staticmethod
    def _read_eps_from_profile(path: str) -> int | None:
        """profiles.yml에서 eps 값을 추출한다. (간단 파서)"""
        try:
            with open(path, "r", encoding="utf-8") as f:
                for line in f:
                    stripped = line.strip()
                    if not stripped or stripped.startswith("#"):
                        continue
                    if stripped.startswith("eps:"):
                        value = stripped.split(":", 1)[1].strip().strip("'\"")
                        if not value:
                            return None
                        try:
                            return int(float(value))
                        except ValueError:
                            return None
            return None
        except FileNotFoundError:
            return None

    @staticmethod
    def _parse_duration_seconds(value: str | None) -> float | None:
        """문자열 기간(예: '3 seconds', '5s', '1500ms')을 초로 변환한다."""
        if not value:
            return None
        raw = value.strip().lower()
        if not raw:
            return None
        if raw.isdigit():
            return float(raw)

        match = re.match(r"^([0-9]+(?:\\.[0-9]+)?)\\s*([a-z]+)$", raw)
        if not match:
            parts = raw.split()
            if len(parts) == 2 and parts[0].replace(".", "", 1).isdigit():
                number = float(parts[0])
                unit = parts[1]
            else:
                return None
        else:
            number = float(match.group(1))
            unit = match.group(2)

        unit_map = {
            "ms": 1 / 1000,
            "millisecond": 1 / 1000,
            "milliseconds": 1 / 1000,
            "s": 1,
            "sec": 1,
            "secs": 1,
            "second": 1,
            "seconds": 1,
            "m": 60,
            "min": 60,
            "mins": 60,
            "minute": 60,
            "minutes": 60,
            "h": 3600,
            "hr": 3600,
            "hour": 3600,
            "hours": 3600,
        }
        if unit not in unit_map:
            return None
        return number * unit_map[unit]

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
