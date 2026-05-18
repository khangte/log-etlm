from __future__ import annotations

from dataclasses import dataclass
import os
from typing import Mapping

from common.get_env import (
    get_env_bool,
    get_env_float,
    get_env_int,
    get_env_optional_str,
    get_env_str,
)


@dataclass(frozen=True)
class StreamIngestSettings:
    """스트리밍 적재 설정을 담는다."""
    kafka_bootstrap: str
    fact_topics: str
    dlq_topic: str | None
    starting_offsets: str | None
    max_offsets_per_trigger: str | None
    max_offsets_cap: int | None
    target_eps: int | None
    max_offsets_safety: float | None
    target_eps_profile_path: str | None
    kafka_min_partitions: int | None
    kafka_min_partitions_multiplier: float | None
    enable_dlq_stream: bool
    reset_checkpoint_on_start: bool


def load_stream_ingest_settings(
    env: Mapping[str, str] | None = None,
) -> StreamIngestSettings:
    """환경 변수에서 스트리밍 적재 설정을 로드한다."""
    source = env or os.environ
    kafka_bootstrap = get_env_str(source, "KAFKA_BOOTSTRAP", "") or ""
    fact_topics = get_env_str(source, "SPARK_FACT_TOPICS", "") or ""
    return StreamIngestSettings(
        kafka_bootstrap=kafka_bootstrap,
        fact_topics=fact_topics,
        dlq_topic=get_env_optional_str(source, "SPARK_DLQ_TOPIC"),
        starting_offsets=get_env_optional_str(source, "SPARK_STARTING_OFFSETS"),
        max_offsets_per_trigger=get_env_str(source, "SPARK_MAX_OFFSETS_PER_TRIGGER"),
        max_offsets_cap=get_env_int(source, "SPARK_MAX_OFFSETS_CAP"),
        target_eps=get_env_int(source, "SPARK_TARGET_EPS"),
        max_offsets_safety=get_env_float(source, "SPARK_MAX_OFFSETS_SAFETY"),
        target_eps_profile_path=get_env_optional_str(
            source, "SPARK_TARGET_EPS_PROFILE_PATH"
        ),
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
