# -----------------------------------------------------------------------------
# 파일명 : log_simulator/publisher/settings.py
# 목적   : 퍼블리셔 워커 설정
# -----------------------------------------------------------------------------

from __future__ import annotations

from dataclasses import dataclass
import os


@dataclass(frozen=True)
class PublisherSettings:
    workers: int = int(os.getenv("PUBLISHER_WORKERS", "8"))
    worker_batch_size: int = int(os.getenv("WORKER_BATCH_SIZE", "80"))
    queue_warn_ratio: float = float(os.getenv("PUBLISH_QUEUE_WARN_RATIO", "0.7"))
    idle_warn_sec: float = float(os.getenv("IDLE_WARN_SEC", "0.2"))
    send_warn_sec: float = float(os.getenv("SEND_WARN_SEC", "0.3"))
    retry_backoff_sec: float = float(os.getenv("PUBLISH_RETRY_BACKOFF_SEC", "0.2"))


PUBLISHER_SETTINGS = PublisherSettings()
