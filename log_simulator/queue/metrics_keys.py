# -----------------------------------------------------------------------------
# file: log_simulator/queue/metrics_keys.py
# purpose: shared metric key constants
# -----------------------------------------------------------------------------

from __future__ import annotations

GENERATED_TOTAL = "generated_total"
ENQUEUED_TOTAL = "enqueued_total"
OVERFLOW_DROPPED_TOTAL = "overflow_dropped_total"

PUBLISHED_TOTAL = "published_total"
PUBLISH_FAIL_TOTAL = "publish_fail_total"
PUBLISH_FAIL_DROP_TOTAL = "publish_fail_drop_total"

QUEUE_DEPTH = "queue_depth"
QUEUE_WAIT_SUM_MS = "queue_wait_sum_ms"
QUEUE_WAIT_COUNT = "queue_wait_count"

PUBLISH_LATENCY_SUM_MS = "publish_latency_sum_ms"
PUBLISH_LATENCY_COUNT = "publish_latency_count"

GEN_DURATION_SUM_MS = "gen_duration_sum_ms"
GEN_DURATION_COUNT = "gen_duration_count"
BUILD_DURATION_SUM_MS = "build_duration_sum_ms"
BUILD_DURATION_COUNT = "build_duration_count"
ENQUEUE_DURATION_SUM_MS = "enqueue_duration_sum_ms"
ENQUEUE_DURATION_COUNT = "enqueue_duration_count"
