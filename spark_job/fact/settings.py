from __future__ import annotations

from dataclasses import dataclass
from functools import lru_cache
import os
from typing import Mapping

from common.get_env import get_env_bool, get_env_int


FACT_EVENT_TABLE = "analytics.event_log"
FACT_EVENT_CHECKPOINT_DIR = "/data/log-etlm/spark_checkpoints/event_log"


@dataclass(frozen=True)
class FactStreamSettings:
    """팩트 이벤트 스트림 설정을 담는다."""
    table_name: str
    checkpoint_dir: str
    trigger_interval_sec: int | None
    pre_coalesce_partitions: int | None
    skip_empty_batch: bool


def load_fact_stream_settings(
    env: Mapping[str, str] | None = None,
) -> FactStreamSettings:
    """환경 변수에서 팩트 스트림 설정을 로드한다."""
    source = env or os.environ
    return FactStreamSettings(
        table_name=FACT_EVENT_TABLE,
        checkpoint_dir=FACT_EVENT_CHECKPOINT_DIR,
        trigger_interval_sec=get_env_int(source, "SPARK_FACT_TRIGGER_INTERVAL_SEC"),
        pre_coalesce_partitions=get_env_int(source, "SPARK_FACT_PRE_COALESCE_PARTITIONS"),
        skip_empty_batch=get_env_bool(source, "SPARK_SKIP_EMPTY_BATCH", False),
    )


@lru_cache(maxsize=1)
def get_fact_stream_settings() -> FactStreamSettings:
    """캐시된 팩트 스트림 설정을 반환한다."""
    return load_fact_stream_settings()
