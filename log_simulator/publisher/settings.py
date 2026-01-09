from __future__ import annotations

import os
from dataclasses import dataclass


def _read_retry_backoff_sec() -> float:
    raw_ms = os.getenv("RETRY_BACKOFF_MS")
    if raw_ms is not None:
        try:
            return float(raw_ms) / 1000.0
        except ValueError:
            return 0.05
    return float(os.getenv("PUBLISH_RETRY_BACKOFF_SEC", "0.2"))


def _read_batch_wait_sec() -> float:
    raw_ms = os.getenv("BATCH_WAIT_MS", os.getenv("WORKER_BATCH_WAIT_MS", "20"))
    try:
        return float(raw_ms) / 1000.0
    except ValueError:
        return 0.02


@dataclass(frozen=True)
class PublisherSettings:
    workers: int = int(os.getenv("PUBLISHER_WORKERS", "8"))
    batch_size: int = int(os.getenv("BATCH_SIZE", os.getenv("WORKER_BATCH_SIZE", "80")))
    worker_batch_size: int = int(os.getenv("WORKER_BATCH_SIZE", "80"))
    batch_wait_sec: float = _read_batch_wait_sec()
    queue_warn_ratio: float = float(os.getenv("PUBLISH_QUEUE_WARN_RATIO", os.getenv("QUEUE_WARN_RATIO", "0.7")))
    idle_warn_sec: float = float(os.getenv("IDLE_WARN_SEC", "0.2"))
    send_warn_sec: float = float(os.getenv("SEND_WARN_SEC", "0.3"))
    retry_backoff_sec: float = _read_retry_backoff_sec()
    retry_max: int = int(os.getenv("RETRY_MAX", "1"))


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


PUBLISHER_SETTINGS = PublisherSettings()
PRODUCER_SETTINGS = ProducerSettings()
