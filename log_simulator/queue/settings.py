from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class MetricsSettings:
    interval_sec: float = float(os.getenv("METRICS_INTERVAL_SEC", "10"))
    queue_size: int = int(os.getenv("METRICS_QUEUE_SIZE", "10000"))


METRICS_SETTINGS = MetricsSettings()
