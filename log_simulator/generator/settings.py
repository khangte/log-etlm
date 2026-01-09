from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class SimulatorSettings:
    simulator_share: float = float(os.getenv("SIMULATOR_SHARE", "1.0"))
    log_batch_size: int = int(os.getenv("LOG_BATCH_SIZE", "100"))
    tick_sec: float = float(os.getenv("TICK_SEC", "0.1"))
    queue_size: int = int(os.getenv("QUEUE_MAX_SIZE", os.getenv("QUEUE_SIZE", "2000")))
    overflow_policy: str = os.getenv("OVERFLOW_POLICY", "drop_oldest")
    queue_metric_interval_sec: float = float(os.getenv("QUEUE_METRIC_INTERVAL_SEC", "1.0"))
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
    event_mode: str = os.getenv("SIM_EVENT_MODE", "domain")


SIMULATOR_SETTINGS = SimulatorSettings()
