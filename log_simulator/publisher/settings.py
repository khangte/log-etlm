# -----------------------------------------------------------------------------
# 파일명 : log_simulator/publisher/settings.py
# 목적   : 퍼블리셔 워커 설정
# -----------------------------------------------------------------------------

from __future__ import annotations

from dataclasses import dataclass
import os


@dataclass(frozen=True)
class PublisherSettings:
    workers: int = int(os.getenv("PUBLISHER_WORKERS"))
    worker_batch_size: int = int(os.getenv("WORKER_BATCH_SIZE"))
    queue_warn_ratio: float = float(os.getenv("PUBLISH_QUEUE_WARN_RATIO"))
    idle_warn_sec: float = float(os.getenv("IDLE_WARN_SEC"))
    send_warn_sec: float = float(os.getenv("SEND_WARN_SEC"))
    retry_backoff_sec: float = float(os.getenv("PUBLISH_RETRY_BACKOFF_SEC"))


PUBLISHER_SETTINGS = PublisherSettings()
