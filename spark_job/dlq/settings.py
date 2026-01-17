from __future__ import annotations

from dataclasses import dataclass
import os
from typing import Mapping, Optional

from common.get_env import get_env_bool, get_env_str


FACT_EVENT_DLQ_TABLE = "analytics.fact_event_dlq"
FACT_EVENT_DLQ_CHECKPOINT_DIR = "/data/log-etlm/spark_checkpoints/fact_event_dlq"
FACT_EVENT_DLQ_KAFKA_CHECKPOINT_DIR = "/data/log-etlm/spark_checkpoints/fact_event_dlq_kafka"


@dataclass(frozen=True)
class DlqStreamSettings:
    """DLQ 스트림 설정을 담는다."""
    table_name: str
    checkpoint_dir: str
    trigger_interval: Optional[str]


@dataclass(frozen=True)
class DlqKafkaSettings:
    """DLQ Kafka 적재 설정을 담는다."""
    bootstrap: str
    checkpoint_dir: str
    trigger_interval: Optional[str]
    log_empty: bool


def load_dlq_stream_settings(
    env: Mapping[str, str] | None = None,
) -> DlqStreamSettings:
    """환경 변수에서 DLQ 스트림 설정을 로드한다."""
    source = env or os.environ
    return DlqStreamSettings(
        table_name=FACT_EVENT_DLQ_TABLE,
        checkpoint_dir=FACT_EVENT_DLQ_CHECKPOINT_DIR,
        trigger_interval=get_env_str(source, "SPARK_DLQ_TRIGGER_INTERVAL"),
    )


def load_dlq_kafka_settings(
    env: Mapping[str, str] | None = None,
) -> DlqKafkaSettings:
    """환경 변수에서 DLQ Kafka 설정을 로드한다."""
    source = env or os.environ
    return DlqKafkaSettings(
        bootstrap=get_env_str(source, "KAFKA_BOOTSTRAP", "") or "",
        checkpoint_dir=FACT_EVENT_DLQ_KAFKA_CHECKPOINT_DIR,
        trigger_interval=get_env_str(source, "SPARK_DLQ_KAFKA_TRIGGER_INTERVAL"),
        log_empty=get_env_bool(source, "SPARK_DLQ_KAFKA_LOG_EMPTY", False),
    )


_dlq_stream_settings_cache: DlqStreamSettings | None = None
_dlq_kafka_settings_cache: DlqKafkaSettings | None = None


def get_dlq_stream_settings() -> DlqStreamSettings:
    """캐시된 DLQ 스트림 설정을 반환한다."""
    global _dlq_stream_settings_cache
    if _dlq_stream_settings_cache is None:
        _dlq_stream_settings_cache = load_dlq_stream_settings()
    return _dlq_stream_settings_cache


def get_dlq_kafka_settings() -> DlqKafkaSettings:
    """캐시된 DLQ Kafka 설정을 반환한다."""
    global _dlq_kafka_settings_cache
    if _dlq_kafka_settings_cache is None:
        _dlq_kafka_settings_cache = load_dlq_kafka_settings()
    return _dlq_kafka_settings_cache
