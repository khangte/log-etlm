from __future__ import annotations

from dataclasses import dataclass
import os
from typing import Mapping, Optional

from common.get_env import get_env_bool, get_env_str


@dataclass(frozen=True)
class StreamIngestSettings:
    """스트리밍 적재 설정을 담는다."""
    kafka_bootstrap: str
    fact_topics: str
    dlq_topic: Optional[str]
    starting_offsets: Optional[str]
    max_offsets_per_trigger: Optional[str]
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
