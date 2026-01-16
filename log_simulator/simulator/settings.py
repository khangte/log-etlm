# -----------------------------------------------------------------------------
# 파일명 : log_simulator/simulator/settings.py
# 목적   : 시뮬레이터 런타임 설정
# -----------------------------------------------------------------------------

from __future__ import annotations

from dataclasses import dataclass
import os


@dataclass(frozen=True)
class SimulatorSettings:
    simulator_share: float = float(os.getenv("SIMULATOR_SHARE"))
    log_batch_size: int = int(os.getenv("LOG_BATCH_SIZE"))
    tick_sec: float = float(os.getenv("TICK_SEC"))
    queue_size: int = int(os.getenv("QUEUE_SIZE"))
    loops_per_service: int = int(os.getenv("LOOPS_PER_SERVICE"))
    queue_warn_ratio: float = float(os.getenv("SIM_QUEUE_WARN_RATIO"))
    queue_low_watermark_ratio: float = float(os.getenv("QUEUE_LOW_WATERMARK_RATIO"))
    queue_low_sleep_scale: float = float(os.getenv("QUEUE_LOW_SLEEP_SCALE"))
    queue_throttle_ratio: float = float(os.getenv("QUEUE_THROTTLE_RATIO"))
    queue_resume_ratio: float = float(os.getenv("QUEUE_RESUME_RATIO"))
    queue_throttle_sleep: float = float(os.getenv("QUEUE_THROTTLE_SLEEP"))
    queue_soft_throttle_ratio: float = float(os.getenv("QUEUE_SOFT_THROTTLE_RATIO"))
    queue_soft_resume_ratio: float = float(os.getenv("QUEUE_SOFT_RESUME_RATIO"))
    queue_soft_scale_step: float = float(os.getenv("QUEUE_SOFT_SCALE_STEP"))
    queue_soft_scale_min: float = float(os.getenv("QUEUE_SOFT_SCALE_MIN"))
    queue_soft_scale_max: float = float(os.getenv("QUEUE_SOFT_SCALE_MAX"))
    behind_log_every_sec: float = float(os.getenv("SIM_BEHIND_LOG_EVERY_SEC"))
    shutdown_drain_timeout_sec: float = float(os.getenv("SIM_DRAIN_TIMEOUT_SEC"))
    event_mode: str = os.getenv("SIM_EVENT_MODE")  # all | domain | http


@dataclass(frozen=True)
class QueueThrottleConfig:
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


SIMULATOR_SETTINGS = SimulatorSettings()
QUEUE_CONFIG = QueueThrottleConfig(
    warn_ratio=SIMULATOR_SETTINGS.queue_warn_ratio,
    low_watermark_ratio=SIMULATOR_SETTINGS.queue_low_watermark_ratio,
    low_sleep_scale=SIMULATOR_SETTINGS.queue_low_sleep_scale,
    throttle_ratio=SIMULATOR_SETTINGS.queue_throttle_ratio,
    resume_ratio=SIMULATOR_SETTINGS.queue_resume_ratio,
    throttle_sleep=SIMULATOR_SETTINGS.queue_throttle_sleep,
    soft_throttle_ratio=SIMULATOR_SETTINGS.queue_soft_throttle_ratio,
    soft_resume_ratio=SIMULATOR_SETTINGS.queue_soft_resume_ratio,
    soft_scale_step=SIMULATOR_SETTINGS.queue_soft_scale_step,
    soft_scale_min=SIMULATOR_SETTINGS.queue_soft_scale_min,
    soft_scale_max=SIMULATOR_SETTINGS.queue_soft_scale_max,
)
