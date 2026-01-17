# -----------------------------------------------------------------------------
# 파일명 : log_simulator/publisher/settings.py
# 목적   : 퍼블리셔 워커 설정
# -----------------------------------------------------------------------------

from __future__ import annotations

from dataclasses import dataclass
import os
from typing import Mapping

from common.get_env import get_env_float, get_env_int


@dataclass(frozen=True)
class PublisherSettings:
    """퍼블리셔 워커 설정 값을 담는다."""
    workers: int
    worker_batch_size: int
    queue_warn_ratio: float
    idle_warn_sec: float
    send_warn_sec: float
    retry_backoff_sec: float


_DEFAULT_WORKERS = 5
_DEFAULT_WORKER_BATCH_SIZE = 500
_DEFAULT_QUEUE_WARN_RATIO = 0.7
_DEFAULT_IDLE_WARN_SEC = 5.0
_DEFAULT_SEND_WARN_SEC = 5.0
_DEFAULT_RETRY_BACKOFF_SEC = 0.2


def load_publisher_settings(env: Mapping[str, str] | None = None) -> PublisherSettings:
    """환경 변수에서 퍼블리셔 설정을 로드한다."""
    source = env or os.environ
    return PublisherSettings(
        workers=get_env_int(source, "PUBLISHER_WORKERS", _DEFAULT_WORKERS),
        worker_batch_size=get_env_int(
            source,
            "WORKER_BATCH_SIZE",
            _DEFAULT_WORKER_BATCH_SIZE,
        ),
        queue_warn_ratio=get_env_float(
            source,
            "PUBLISH_QUEUE_WARN_RATIO",
            _DEFAULT_QUEUE_WARN_RATIO,
        ),
        idle_warn_sec=get_env_float(
            source,
            "IDLE_WARN_SEC",
            _DEFAULT_IDLE_WARN_SEC,
        ),
        send_warn_sec=get_env_float(
            source,
            "SEND_WARN_SEC",
            _DEFAULT_SEND_WARN_SEC,
        ),
        retry_backoff_sec=get_env_float(
            source,
            "PUBLISH_RETRY_BACKOFF_SEC",
            _DEFAULT_RETRY_BACKOFF_SEC,
        ),
    )


_settings_cache: PublisherSettings | None = None


def get_publisher_settings() -> PublisherSettings:
    """캐시된 퍼블리셔 설정을 반환한다."""
    global _settings_cache
    if _settings_cache is None:
        _settings_cache = load_publisher_settings()
    return _settings_cache
