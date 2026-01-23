from __future__ import annotations

from dataclasses import dataclass
import os
from typing import Mapping, Optional

from common.get_env import get_env_bool, get_env_int, get_env_str


FACT_EVENT_TABLE = "analytics.fact_event"
FACT_EVENT_CHECKPOINT_DIR = "/data/log-etlm/spark_checkpoints/fact_event"


@dataclass(frozen=True)
class FactStreamSettings:
    """팩트 이벤트 스트림 설정을 담는다."""
    table_name: str
    checkpoint_dir: str
    trigger_interval: Optional[str]
    store_raw_json: bool
    num_partitions: Optional[int]


def load_fact_stream_settings(
    env: Mapping[str, str] | None = None,
) -> FactStreamSettings:
    """환경 변수에서 팩트 스트림 설정을 로드한다."""
    source = env or os.environ
    return FactStreamSettings(
        table_name=FACT_EVENT_TABLE,
        checkpoint_dir=FACT_EVENT_CHECKPOINT_DIR,
        trigger_interval=get_env_str(source, "SPARK_FACT_TRIGGER_INTERVAL"),
        store_raw_json=get_env_bool(source, "SPARK_STORE_RAW_JSON", False),
        num_partitions=get_env_int(source, "SPARK_CLICKHOUSE_WRITE_PARTITIONS"),
    )


_settings_cache: FactStreamSettings | None = None


def get_fact_stream_settings() -> FactStreamSettings:
    """캐시된 팩트 스트림 설정을 반환한다."""
    global _settings_cache
    if _settings_cache is None:
        _settings_cache = load_fact_stream_settings()
    return _settings_cache
