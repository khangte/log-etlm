# -----------------------------------------------------------------------------
# file: log_simulator/publisher/metrics_reporter.py
# purpose: metrics reporter for split publisher entrypoint
# -----------------------------------------------------------------------------

from __future__ import annotations

from collections import defaultdict
import logging
import os
import queue as std_queue
import time
from typing import Any, Dict

from ..config.settings import METRICS_SETTINGS
from ..queue.metrics_keys import (
    ENQUEUED_TOTAL,
    GENERATED_TOTAL,
    OVERFLOW_DROPPED_TOTAL,
    PUBLISHED_TOTAL,
    PUBLISH_FAIL_DROP_TOTAL,
    QUEUE_DEPTH,
    QUEUE_WAIT_COUNT,
    QUEUE_WAIT_SUM_MS,
)


def run_metrics_reporter(metrics_queue: Any, stop_event: Any | None) -> None:
    log = logging.getLogger("log_simulator.metrics")
    log.setLevel(logging.INFO)
    if not log.handlers:
        handler = logging.StreamHandler()
        handler.setFormatter(
            logging.Formatter("%(asctime)s [%(levelname)s] %(name)s - %(message)s")
        )
        log.addHandler(handler)

    interval_sec = max(METRICS_SETTINGS.interval_sec, 1.0)
    last_report = time.time()
    window_counters: Dict[str, Dict[str | None, int]] = defaultdict(lambda: defaultdict(int))
    gauges: Dict[str, float] = {}
    idle_rounds = 0

    def _sum_metric(name: str) -> int:
        return sum(window_counters.get(name, {}).values())

    def _svc_breakdown(name: str) -> str:
        items = window_counters.get(name, {})
        if not items:
            return ""
        pairs = [f"{svc}:{cnt}" for svc, cnt in sorted(items.items()) if svc]
        return ", ".join(pairs)

    while True:
        now = time.time()
        if (now - last_report) >= interval_sec:
            elapsed = now - last_report
            gen_total = _sum_metric(GENERATED_TOTAL)
            enq_total = _sum_metric(ENQUEUED_TOTAL)
            drop_total = _sum_metric(OVERFLOW_DROPPED_TOTAL)
            pub_total = _sum_metric(PUBLISHED_TOTAL)
            fail_drop_total = _sum_metric(PUBLISH_FAIL_DROP_TOTAL)
            wait_sum = _sum_metric(QUEUE_WAIT_SUM_MS)
            wait_count = _sum_metric(QUEUE_WAIT_COUNT)
            wait_avg = (wait_sum / wait_count) if wait_count > 0 else 0.0

            log.info(
                "[metrics pid=%d] gen_eps=%.1f enq_eps=%.1f pub_eps=%.1f drop_eps=%.1f "
                "fail_drop_eps=%.1f queue_depth=%s queue_wait_avg_ms=%.1f gen_by_svc=(%s) "
                "pub_by_svc=(%s)",
                os.getpid(),
                gen_total / elapsed if elapsed > 0 else 0.0,
                enq_total / elapsed if elapsed > 0 else 0.0,
                pub_total / elapsed if elapsed > 0 else 0.0,
                drop_total / elapsed if elapsed > 0 else 0.0,
                fail_drop_total / elapsed if elapsed > 0 else 0.0,
                gauges.get("queue_depth", 0),
                wait_avg,
                _svc_breakdown(GENERATED_TOTAL),
                _svc_breakdown(PUBLISHED_TOTAL),
            )

            window_counters = defaultdict(lambda: defaultdict(int))
            last_report = now

        try:
            item = metrics_queue.get(timeout=0.5)
        except TypeError:
            item = metrics_queue.get()
        except std_queue.Empty:
            idle_rounds += 1
            if stop_event is not None and stop_event.is_set() and idle_rounds >= 3:
                break
            continue

        if item is None:
            break

        name, value, service = item
        idle_rounds = 0

        if name == QUEUE_DEPTH:
            gauges[name] = float(value)
        else:
            window_counters[name][service] += int(value)
