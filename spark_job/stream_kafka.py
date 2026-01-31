from __future__ import annotations

import os
from dataclasses import dataclass, field

from pyspark.sql import DataFrame, SparkSession

from .fact.settings import FactStreamSettings
from .stream_settings import StreamIngestSettings
from .stream_utils import parse_duration_seconds, read_eps_from_profile


@dataclass
class KafkaStreamBuilder:
    settings: StreamIngestSettings
    fact_settings: FactStreamSettings
    _partition_count_cache: dict[str, int] = field(
        default_factory=dict, init=False, repr=False
    )

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
            capped = self._apply_max_offsets_cap(max_offsets)
            if capped and capped != max_offsets:
                print(f"[ℹ️ spark] maxOffsetsPerTrigger(cap)={capped}")
            max_offsets = capped or max_offsets
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

    def _fetch_kafka_partition_count(
        self, spark: SparkSession, topics: str
    ) -> int | None:
        """Kafka 메타데이터에서 파티션 수를 조회한다."""
        cached = self._partition_count_cache.get(topics)
        if cached is not None:
            return cached
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
                total = int(total)
                self._partition_count_cache[topics] = total
                return total
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
            print(f"[⚠️ spark] maxOffsetsPerTrigger cap skipped: invalid value={value}")
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
