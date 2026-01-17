# -----------------------------------------------------------------------------
# 파일명 : log_simulator/producer/settings.py
# 목적   : Kafka producer 설정 로더(env → ProducerSettings)
# -----------------------------------------------------------------------------

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Any, Dict, Mapping, Optional

from common.get_env import get_env_bool, get_env_int, get_env_str


@dataclass(frozen=True)
class ProducerSettings:
    """프로듀서 설정 값을 담는다."""
    brokers: str
    client_id: Optional[str]
    linger_ms: int
    batch_num_messages: int
    queue_buffering_max_kbytes: int
    queue_buffering_max_messages: int
    enable_idempotence: bool
    acks: Optional[str]
    compression_type: Optional[str]

    def to_kafka_config(self) -> Dict[str, Any]:
        """Kafka 설정 딕셔너리를 만든다."""
        if not self.brokers:
            raise ValueError("KAFKA_BOOTSTRAP is required to build Kafka config")

        config: Dict[str, Any] = {
            "bootstrap.servers": self.brokers,
            "enable.idempotence": self.enable_idempotence,
            "max.in.flight.requests.per.connection": 5,
            "linger.ms": self.linger_ms,
            "batch.num.messages": self.batch_num_messages,
            "queue.buffering.max.kbytes": self.queue_buffering_max_kbytes,
            "queue.buffering.max.messages": self.queue_buffering_max_messages,
            "partitioner": "murmur2_random",
        }

        if self.client_id:
            config["client.id"] = self.client_id
        if self.acks:
            config["acks"] = self.acks
        if self.compression_type:
            config["compression.type"] = self.compression_type

        return config


_DEFAULT_LINGER_MS = 20
_DEFAULT_BATCH_NUM_MESSAGES = 5000
_DEFAULT_QUEUE_MAX_KBYTES = 262144
_DEFAULT_QUEUE_MAX_MESSAGES = 2000000


def load_producer_settings(env: Mapping[str, str] | None = None) -> ProducerSettings:
    """환경 변수에서 프로듀서 설정을 로드한다."""
    source = env or os.environ
    return ProducerSettings(
        brokers=get_env_str(source, "KAFKA_BOOTSTRAP", ""),
        client_id=get_env_str(source, "KAFKA_CLIENT_ID"),
        linger_ms=get_env_int(source, "PRODUCER_LINGER_MS", _DEFAULT_LINGER_MS),
        batch_num_messages=get_env_int(
            source,
            "PRODUCER_BATCH_NUM_MESSAGES",
            _DEFAULT_BATCH_NUM_MESSAGES,
        ),
        queue_buffering_max_kbytes=get_env_int(
            source,
            "PRODUCER_QUEUE_MAX_KBYTES",
            _DEFAULT_QUEUE_MAX_KBYTES,
        ),
        queue_buffering_max_messages=get_env_int(
            source,
            "PRODUCER_QUEUE_MAX_MESSAGES",
            _DEFAULT_QUEUE_MAX_MESSAGES,
        ),
        enable_idempotence=get_env_bool(
            source,
            "PRODUCER_ENABLE_IDEMPOTENCE",
            True,
        ),
        acks=get_env_str(source, "PRODUCER_ACKS"),
        compression_type=get_env_str(source, "PRODUCER_COMPRESSION"),
    )


_settings_cache: ProducerSettings | None = None


def get_producer_settings() -> ProducerSettings:
    """캐시된 프로듀서 설정을 반환한다."""
    global _settings_cache
    if _settings_cache is None:
        _settings_cache = load_producer_settings()
    return _settings_cache
