from __future__ import annotations

from dataclasses import dataclass
from functools import lru_cache
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
    starting_offsets: str | None
    max_offsets_per_trigger: str | None
    max_offsets_cap: int | None
    target_eps: int | None
    max_offsets_safety: float | None
    target_eps_profile_path: str | None
    kafka_min_partitions: int | None
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
        starting_offsets=get_env_str(source, "SPARK_STARTING_OFFSETS"),
        max_offsets_per_trigger=get_env_str(source, "SPARK_MAX_OFFSETS_PER_TRIGGER"),
        max_offsets_cap=get_env_int(source, "SPARK_MAX_OFFSETS_CAP"),
        target_eps=get_env_int(source, "SPARK_TARGET_EPS"),
        max_offsets_safety=get_env_float(source, "SPARK_MAX_OFFSETS_SAFETY"),
        target_eps_profile_path=get_env_optional_str(
            source, "SPARK_TARGET_EPS_PROFILE_PATH"
        ),
        kafka_min_partitions=get_env_int(source, "SPARK_KAFKA_MIN_PARTITIONS"),
        reset_checkpoint_on_start=get_env_bool(
            source,
            "SPARK_RESET_CHECKPOINT_ON_START",
            False,
        ),
    )


@lru_cache(maxsize=1)
def get_stream_ingest_settings() -> StreamIngestSettings:
    """캐시된 스트리밍 적재 설정을 반환한다."""
    return load_stream_ingest_settings()
