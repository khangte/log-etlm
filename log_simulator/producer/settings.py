# -----------------------------------------------------------------------------
# 파일명 : log_simulator/producer/settings.py
# 목적   : Kafka producer 설정
# -----------------------------------------------------------------------------

from __future__ import annotations

from dataclasses import dataclass
import os


@dataclass(frozen=True)
class ProducerSettings:
    brokers: str = os.getenv("KAFKA_BOOTSTRAP")
    client_id: str = os.getenv("KAFKA_CLIENT_ID")
    linger_ms: int = int(os.getenv("PRODUCER_LINGER_MS", "5"))
    batch_num_messages: int = int(os.getenv("PRODUCER_BATCH_NUM_MESSAGES", "1000"))
    queue_buffering_max_kbytes: int = int(os.getenv("PRODUCER_QUEUE_MAX_KBYTES", str(128 * 1024)))
    queue_buffering_max_messages: int = int(os.getenv("PRODUCER_QUEUE_MAX_MESSAGES", "500000"))
    enable_idempotence: bool = os.getenv("PRODUCER_ENABLE_IDEMPOTENCE", "true").strip().lower() in (
        "1",
        "true",
        "yes",
        "y",
    )
    acks: str = os.getenv("PRODUCER_ACKS", "all")
    compression_type: str = os.getenv("PRODUCER_COMPRESSION", "snappy")


PRODUCER_SETTINGS = ProducerSettings()
