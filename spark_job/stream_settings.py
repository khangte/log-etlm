from __future__ import annotations

from dataclasses import dataclass
import os
from typing import Mapping, Optional

from common.get_env import get_env_bool, get_env_float, get_env_int, get_env_str


@dataclass(frozen=True)
class StreamIngestSettings:
    """스트리밍 적재 설정을 담는다."""
    kafka_bootstrap: str
    fact_topics: str
    dlq_topic: Optional[str]
    starting_offsets: Optional[str]
    max_offsets_per_trigger: Optional[str]
    target_eps: Optional[int]
    max_offsets_safety: Optional[float]
    target_eps_profile_path: Optional[str]
    kafka_partition_count: Optional[int]
    kafka_min_partitions: Optional[int]
    kafka_min_partitions_multiplier: Optional[float]
    enable_dlq_stream: bool
    reset_checkpoint_on_start: bool


def load_stream_ingest_settings(
    env: Mapping[str, str] | None = None,
) -> StreamIngestSettings:
    """환경 변수에서 스트리밍 적재 설정을 로드한다."""
    source = env or os.environ
    return StreamIngestSettings(
        kafka_bootstrap=get_env_str(source, "KAFKA_BOOTSTRAP", "") or "",
        fact_topics=get_env_str(source, "SPARK_FACT_TOPICS", "") or "",
        dlq_topic=get_env_str(source, "SPARK_DLQ_TOPIC"),
        starting_offsets=get_env_str(source, "SPARK_STARTING_OFFSETS"),
        max_offsets_per_trigger=get_env_str(source, "SPARK_MAX_OFFSETS_PER_TRIGGER"),
        target_eps=get_env_int(source, "SPARK_TARGET_EPS"),
        max_offsets_safety=get_env_float(source, "SPARK_MAX_OFFSETS_SAFETY"),
        target_eps_profile_path=get_env_str(
            source, "SPARK_TARGET_EPS_PROFILE_PATH"
        ),
        kafka_partition_count=get_env_int(source, "SPARK_KAFKA_PARTITION_COUNT"),
        kafka_min_partitions=get_env_int(source, "SPARK_KAFKA_MIN_PARTITIONS"),
        kafka_min_partitions_multiplier=get_env_float(
            source, "SPARK_KAFKA_MIN_PARTITIONS_MULTIPLIER"
        ),
        enable_dlq_stream=get_env_bool(source, "SPARK_ENABLE_DLQ_STREAM", True),
        reset_checkpoint_on_start=get_env_bool(
            source,
            "SPARK_RESET_CHECKPOINT_ON_START",
            False,
        ),
    )


_settings_cache: StreamIngestSettings | None = None


def get_stream_ingest_settings() -> StreamIngestSettings:
    """캐시된 스트리밍 적재 설정을 반환한다."""
    global _settings_cache
    if _settings_cache is None:
        _settings_cache = load_stream_ingest_settings()
    return _settings_cache
