from __future__ import annotations

import logging
import os
from dataclasses import dataclass

from pyspark.sql import DataFrame, SparkSession

from .fact.settings import FactStreamSettings
from .stream_settings import StreamIngestSettings
from .stream_utils import parse_duration_seconds, read_eps_from_profile

logger = logging.getLogger(__name__)


@dataclass
class KafkaStreamBuilder:
    settings: StreamIngestSettings
    fact_settings: FactStreamSettings

    def build_kafka_stream(self, spark: SparkSession, topics: str) -> DataFrame:
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
            logger.info(
                "[INFO] startingOffsets=%s (체크포인트가 있으면 무시될 수 있음)",
                self.settings.starting_offsets,
            )
            reader = reader.option("startingOffsets", self.settings.starting_offsets)
        max_offsets = self.settings.max_offsets_per_trigger
        if not max_offsets:
            auto_offsets = self._compute_max_offsets_per_trigger()
            if auto_offsets:
                max_offsets = str(auto_offsets)
                logger.info("[INFO] maxOffsetsPerTrigger(auto)=%s", max_offsets)
        if max_offsets:
            capped = self._apply_max_offsets_cap(max_offsets)
            if capped and capped != max_offsets:
                logger.info("[INFO] maxOffsetsPerTrigger(cap)=%s", capped)
            max_offsets = capped or max_offsets
            reader = reader.option("maxOffsetsPerTrigger", max_offsets)

        if self.settings.kafka_min_partitions:
            reader = reader.option("minPartitions", str(self.settings.kafka_min_partitions))
        return reader.load()

    def _compute_max_offsets_per_trigger(self) -> int | None:
        """목표 EPS와 트리거 간격으로 maxOffsetsPerTrigger를 계산한다."""
        target_eps = self._resolve_target_eps()
        if not target_eps:
            return None
        trigger_seconds = parse_duration_seconds(self.fact_settings.trigger_interval)
        if not trigger_seconds:
            return None
        safety = self.settings.max_offsets_safety or 1.0
        computed = int(target_eps * trigger_seconds * safety)
        return max(1, computed)

    def _apply_max_offsets_cap(self, value: str) -> str | None:
        """maxOffsetsPerTrigger에 하드 캡을 적용한다."""
        cap = self.settings.max_offsets_cap
        if not cap or cap <= 0:
            return value
        try:
            current = int(float(value))
        except (TypeError, ValueError):
            logger.warning("[WARN] maxOffsetsPerTrigger cap skipped: invalid value=%s", value)
            return value
        if current <= 0:
            return value
        if current > cap:
            return str(max(1, cap))
        return str(max(1, current))

    def _resolve_target_eps(self) -> int | None:
        """target_eps를 환경/프로파일에서 우선순위로 결정한다."""
        if self.settings.target_eps:
            return self.settings.target_eps
        profile_path = self._resolve_target_eps_profile_path()
        if not profile_path:
            return None
        return read_eps_from_profile(profile_path)

    def _resolve_target_eps_profile_path(self) -> str | None:
        """profiles.yml 경로를 결정한다."""
        if self.settings.target_eps_profile_path:
            return self.settings.target_eps_profile_path
        default_path = "/app/log_simulator/config/profiles.yml"
        if os.path.exists(default_path):
            return default_path
        return None
