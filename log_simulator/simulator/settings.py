# -----------------------------------------------------------------------------
# 파일명 : log_simulator/simulator/settings.py
# 목적   : 시뮬레이터 런타임 설정
# -----------------------------------------------------------------------------

from __future__ import annotations

from dataclasses import dataclass
import os
from typing import Mapping, Optional

from common.get_env import get_env_float, get_env_int, get_env_str, get_env_bool


@dataclass(frozen=True)
class SimulatorSettings:
    """시뮬레이터 런타임 설정 값을 담는다."""
    simulator_share: float
    log_batch_size: int
    target_interval_sec: float
    queue_size: int
    loops_per_service: int
    queue_warn_ratio: float
    queue_low_watermark_ratio: float
    queue_low_sleep_scale: float
    queue_throttle_ratio: float
    queue_resume_ratio: float
    queue_throttle_sleep: float
    queue_soft_throttle_ratio: float
    queue_soft_resume_ratio: float
    queue_soft_scale_step: float
    queue_soft_scale_min: float
    queue_soft_scale_max: float
    behind_log_every_sec: float
    shutdown_drain_timeout_sec: float
    event_mode: str  # 전체/도메인/HTTP 모드 구분


@dataclass(frozen=True)
class QueueThrottleConfig:
    """큐 백프레셔 설정 값을 담는다."""
    warn_ratio: float
    low_watermark_ratio: float
    low_sleep_scale: float
    throttle_ratio: float
    resume_ratio: float
    throttle_sleep: float
    soft_throttle_ratio: float
    soft_resume_ratio: float
    soft_scale_step: float
    soft_scale_min: float
    soft_scale_max: float


_DEFAULT_SIMULATOR_SHARE = 1.0
_DEFAULT_LOG_BATCH_SIZE = 240
_DEFAULT_TARGET_INTERVAL_SEC = 0.22
_DEFAULT_QUEUE_SIZE = 4000
_DEFAULT_LOOPS_PER_SERVICE = 4
_DEFAULT_QUEUE_WARN_RATIO = 0.8
_DEFAULT_QUEUE_LOW_WATERMARK_RATIO = 0.2
_DEFAULT_QUEUE_LOW_SLEEP_SCALE = 1.0
_DEFAULT_QUEUE_THROTTLE_RATIO = 0.9
_DEFAULT_QUEUE_RESUME_RATIO = 0.75
_DEFAULT_QUEUE_THROTTLE_SLEEP = 0.05
_DEFAULT_QUEUE_SOFT_THROTTLE_RATIO = 0.85
_DEFAULT_QUEUE_SOFT_RESUME_RATIO = 0.7
_DEFAULT_QUEUE_SOFT_SCALE_STEP = 0.1
_DEFAULT_QUEUE_SOFT_SCALE_MIN = 0.2
_DEFAULT_QUEUE_SOFT_SCALE_MAX = 1.0
_DEFAULT_BEHIND_LOG_EVERY_SEC = 5.0
_DEFAULT_SHUTDOWN_DRAIN_TIMEOUT_SEC = 5.0
_DEFAULT_EVENT_MODE = "domain"


def _normalize_event_mode(mode: Optional[str]) -> str:
    """이벤트 모드를 정규화한다."""
    if not mode:
        return _DEFAULT_EVENT_MODE
    normalized = mode.strip().lower()
    if normalized in ("all", "domain", "http"):
        return normalized
    return _DEFAULT_EVENT_MODE


def load_simulator_settings(env: Mapping[str, str] | None = None) -> SimulatorSettings:
    """환경 변수에서 시뮬레이터 설정을 로드한다."""
    source = env or os.environ
    target_interval_sec = get_env_float(
        source,
        "TARGET_INTERVAL_SEC",
        _DEFAULT_TARGET_INTERVAL_SEC,
    )
    return SimulatorSettings(
        simulator_share=get_env_float(
            source,
            "SIMULATOR_SHARE",
            _DEFAULT_SIMULATOR_SHARE,
        ),
        log_batch_size=get_env_int(
            source,
            "LOG_BATCH_SIZE",
            _DEFAULT_LOG_BATCH_SIZE,
        ),
        target_interval_sec=target_interval_sec,
        queue_size=get_env_int(
            source,
            "QUEUE_SIZE",
            _DEFAULT_QUEUE_SIZE,
        ),
        loops_per_service=get_env_int(
            source,
            "LOOPS_PER_SERVICE",
            _DEFAULT_LOOPS_PER_SERVICE,
        ),
        queue_warn_ratio=get_env_float(
            source,
            "SIM_QUEUE_WARN_RATIO",
            _DEFAULT_QUEUE_WARN_RATIO,
        ),
        queue_low_watermark_ratio=get_env_float(
            source,
            "QUEUE_LOW_WATERMARK_RATIO",
            _DEFAULT_QUEUE_LOW_WATERMARK_RATIO,
        ),
        queue_low_sleep_scale=get_env_float(
            source,
            "QUEUE_LOW_SLEEP_SCALE",
            _DEFAULT_QUEUE_LOW_SLEEP_SCALE,
        ),
        queue_throttle_ratio=get_env_float(
            source,
            "QUEUE_THROTTLE_RATIO",
            _DEFAULT_QUEUE_THROTTLE_RATIO,
        ),
        queue_resume_ratio=get_env_float(
            source,
            "QUEUE_RESUME_RATIO",
            _DEFAULT_QUEUE_RESUME_RATIO,
        ),
        queue_throttle_sleep=get_env_float(
            source,
            "QUEUE_THROTTLE_SLEEP",
            _DEFAULT_QUEUE_THROTTLE_SLEEP,
        ),
        queue_soft_throttle_ratio=get_env_float(
            source,
            "QUEUE_SOFT_THROTTLE_RATIO",
            _DEFAULT_QUEUE_SOFT_THROTTLE_RATIO,
        ),
        queue_soft_resume_ratio=get_env_float(
            source,
            "QUEUE_SOFT_RESUME_RATIO",
            _DEFAULT_QUEUE_SOFT_RESUME_RATIO,
        ),
        queue_soft_scale_step=get_env_float(
            source,
            "QUEUE_SOFT_SCALE_STEP",
            _DEFAULT_QUEUE_SOFT_SCALE_STEP,
        ),
        queue_soft_scale_min=get_env_float(
            source,
            "QUEUE_SOFT_SCALE_MIN",
            _DEFAULT_QUEUE_SOFT_SCALE_MIN,
        ),
        queue_soft_scale_max=get_env_float(
            source,
            "QUEUE_SOFT_SCALE_MAX",
            _DEFAULT_QUEUE_SOFT_SCALE_MAX,
        ),
        behind_log_every_sec=get_env_float(
            source,
            "SIM_BEHIND_LOG_EVERY_SEC",
            _DEFAULT_BEHIND_LOG_EVERY_SEC,
        ),
        shutdown_drain_timeout_sec=get_env_float(
            source,
            "SIM_DRAIN_TIMEOUT_SEC",
            _DEFAULT_SHUTDOWN_DRAIN_TIMEOUT_SEC,
        ),
        event_mode=_normalize_event_mode(
            get_env_str(
                source, 
                "SIM_EVENT_MODE", 
                _DEFAULT_EVENT_MODE
            )
        ),
    )


_settings_cache: SimulatorSettings | None = None
_queue_config_cache: QueueThrottleConfig | None = None


def get_simulator_settings() -> SimulatorSettings:
    """캐시된 시뮬레이터 설정을 반환한다."""
    global _settings_cache
    if _settings_cache is None:
        _settings_cache = load_simulator_settings()
    return _settings_cache


def build_queue_config(settings: SimulatorSettings) -> QueueThrottleConfig:
    """시뮬레이터 설정으로 큐 정책을 생성한다."""
    return QueueThrottleConfig(
        warn_ratio=settings.queue_warn_ratio,
        low_watermark_ratio=settings.queue_low_watermark_ratio,
        low_sleep_scale=settings.queue_low_sleep_scale,
        throttle_ratio=settings.queue_throttle_ratio,
        resume_ratio=settings.queue_resume_ratio,
        throttle_sleep=settings.queue_throttle_sleep,
        soft_throttle_ratio=settings.queue_soft_throttle_ratio,
        soft_resume_ratio=settings.queue_soft_resume_ratio,
        soft_scale_step=settings.queue_soft_scale_step,
        soft_scale_min=settings.queue_soft_scale_min,
        soft_scale_max=settings.queue_soft_scale_max,
    )


def get_queue_config(
    settings: SimulatorSettings | None = None,
) -> QueueThrottleConfig:
    """큐 정책 설정을 반환한다."""
    if settings is None:
        global _queue_config_cache
        if _queue_config_cache is None:
            _queue_config_cache = build_queue_config(get_simulator_settings())
        return _queue_config_cache
    return build_queue_config(settings)
