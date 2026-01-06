# -----------------------------------------------------------------------------
# 파일명 : log_simulator/config/settings.py
# 목적   : 환경변수 기반 런타임 설정을 한 곳에서 관리
# -----------------------------------------------------------------------------

from __future__ import annotations

from dataclasses import dataclass
import os


@dataclass(frozen=True)
class SimulatorSettings:
    simulator_share: float = float(os.getenv("SIMULATOR_SHARE", "1.0"))
    log_batch_size: int = int(os.getenv("LOG_BATCH_SIZE", "100"))
    tick_sec: float = float(os.getenv("TICK_SEC", "0.1"))
    queue_size: int = int(os.getenv("QUEUE_SIZE", "2000"))
    loops_per_service: int = int(os.getenv("LOOPS_PER_SERVICE", "8"))
    queue_warn_ratio: float = float(os.getenv("SIM_QUEUE_WARN_RATIO", os.getenv("QUEUE_WARN_RATIO", "0.8")))
    queue_low_watermark_ratio: float = float(os.getenv("QUEUE_LOW_WATERMARK_RATIO", "0.2"))
    queue_low_sleep_scale: float = float(os.getenv("QUEUE_LOW_SLEEP_SCALE", "0.3"))
    queue_throttle_ratio: float = float(os.getenv("QUEUE_THROTTLE_RATIO", "0.9"))
    queue_resume_ratio: float = float(os.getenv("QUEUE_RESUME_RATIO", "0.75"))
    queue_throttle_sleep: float = float(os.getenv("QUEUE_THROTTLE_SLEEP", "0.05"))
    queue_soft_throttle_ratio: float = float(os.getenv("QUEUE_SOFT_THROTTLE_RATIO", "0.85"))
    queue_soft_resume_ratio: float = float(os.getenv("QUEUE_SOFT_RESUME_RATIO", "0.7"))
    queue_soft_scale_step: float = float(os.getenv("QUEUE_SOFT_SCALE_STEP", "0.1"))
    queue_soft_scale_min: float = float(os.getenv("QUEUE_SOFT_SCALE_MIN", "0.2"))
    queue_soft_scale_max: float = float(os.getenv("QUEUE_SOFT_SCALE_MAX", "1.0"))
    behind_log_every_sec: float = float(os.getenv("SIM_BEHIND_LOG_EVERY_SEC", "5.0"))
    shutdown_drain_timeout_sec: float = float(os.getenv("SIM_DRAIN_TIMEOUT_SEC", "5.0"))
    event_mode: str = os.getenv("SIM_EVENT_MODE", "domain")  # all | domain | http


@dataclass(frozen=True)
class PublisherSettings:
    workers: int = int(os.getenv("PUBLISHER_WORKERS", "8"))
    worker_batch_size: int = int(os.getenv("WORKER_BATCH_SIZE", "80"))
    queue_warn_ratio: float = float(os.getenv("PUBLISH_QUEUE_WARN_RATIO", os.getenv("QUEUE_WARN_RATIO", "0.7")))
    idle_warn_sec: float = float(os.getenv("IDLE_WARN_SEC", "0.2"))
    send_warn_sec: float = float(os.getenv("SEND_WARN_SEC", "0.3"))
    retry_backoff_sec: float = float(os.getenv("PUBLISH_RETRY_BACKOFF_SEC", "0.2"))


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


SIMULATOR_SETTINGS = SimulatorSettings()
PUBLISHER_SETTINGS = PublisherSettings()
PRODUCER_SETTINGS = ProducerSettings()
