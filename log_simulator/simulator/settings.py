# -----------------------------------------------------------------------------
# 파일명 : log_simulator/simulator/settings.py
# 목적   : 시뮬레이터 런타임 설정
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
    queue_warn_ratio: float = float(os.getenv("SIM_QUEUE_WARN_RATIO", "0.8"))
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
