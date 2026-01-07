# -----------------------------------------------------------------------------
# 파일명 : log_simulator/generator/loop_helpers.py
# 목적   : generator 루프 내부 로직을 함수로 분리
# -----------------------------------------------------------------------------

from __future__ import annotations

import math
import time
from typing import Any, Tuple

from ..queue.metrics_keys import (
    BUILD_DURATION_COUNT,
    BUILD_DURATION_SUM_MS,
    ENQUEUED_TOTAL,
    ENQUEUE_DURATION_COUNT,
    ENQUEUE_DURATION_SUM_MS,
    GEN_DURATION_COUNT,
    GEN_DURATION_SUM_MS,
    GENERATED_TOTAL,
    OVERFLOW_DROPPED_TOTAL,
    QUEUE_DEPTH,
)
from ..queue.metrics_sink import emit_metric
from ..queue.queue_adapter import enqueue_batch
from .batch_builder import apply_created_ms, build_batch_items


def compute_batch_size(
    *,
    carry: float,
    scaled_eps: float,
    dt_actual: float,
    max_batch_size: int,
) -> Tuple[int, float]:
    """토큰 버킷 기반으로 배치 크기와 캐리를 계산한다."""
    carry += scaled_eps * dt_actual
    if carry > max_batch_size:
        carry = float(max_batch_size)
    batch_size = int(math.floor(carry))
    if batch_size > max_batch_size:
        batch_size = max_batch_size
    carry -= batch_size
    return batch_size, carry


def enqueue_batch_events(
    *,
    service: str,
    simulator: Any,
    batch_size: int,
    publish_queue: Any,
    metrics_queue: Any | None,
    overflow_policy: str,
    queue_size: int,
) -> float:
    """이벤트를 생성해 큐에 적재하고 측정치를 기록한다."""
    if batch_size <= 0:
        return 0.0

    generate_start = time.perf_counter()
    events = simulator.generate_events(batch_size)
    generate_ms = (time.perf_counter() - generate_start) * 1000.0
    emit_metric(metrics_queue, GEN_DURATION_SUM_MS, int(generate_ms), None)
    emit_metric(metrics_queue, GEN_DURATION_COUNT, 1, None)

    build_start = time.perf_counter()
    created_ms = int(time.time() * 1000)
    apply_created_ms(events, created_ms)
    enqueued_ms = int(time.time() * 1000)
    batch_items = build_batch_items(service, events, enqueued_ms)
    build_ms = (time.perf_counter() - build_start) * 1000.0
    emit_metric(metrics_queue, BUILD_DURATION_SUM_MS, int(build_ms), None)
    emit_metric(metrics_queue, BUILD_DURATION_COUNT, 1, None)

    if batch_items:
        enqueue_start = time.perf_counter()
        emit_metric(metrics_queue, GENERATED_TOTAL, len(batch_items), service)
        enqueued, dropped = enqueue_batch(
            publish_queue=publish_queue,
            batch_items=batch_items,
            overflow_policy=overflow_policy,
            fallback_maxsize=queue_size,
        )
        if enqueued:
            emit_metric(metrics_queue, ENQUEUED_TOTAL, enqueued, service)
        if dropped:
            emit_metric(metrics_queue, OVERFLOW_DROPPED_TOTAL, dropped, service)
        enqueue_ms = (time.perf_counter() - enqueue_start) * 1000.0
        emit_metric(metrics_queue, ENQUEUE_DURATION_SUM_MS, int(enqueue_ms), None)
        emit_metric(metrics_queue, ENQUEUE_DURATION_COUNT, 1, None)
        return (build_ms + enqueue_ms) / 1000.0

    return 0.0


def maybe_emit_queue_depth(
    *,
    metrics_queue: Any | None,
    queue_depth: int,
    interval_sec: float,
    last_metric_ts: float,
) -> float:
    """큐 깊이 게이지를 주기적으로 기록한다."""
    if interval_sec <= 0:
        return last_metric_ts

    now_ts = time.perf_counter()
    if (now_ts - last_metric_ts) >= interval_sec:
        emit_metric(metrics_queue, QUEUE_DEPTH, queue_depth, None)
        return now_ts
    return last_metric_ts


def maybe_log_behind_target(
    *,
    logger: Any,
    loop_start: float,
    last_log_ts: float,
    log_every_sec: float,
    service: str,
    target_eps: float,
    batch_size: int,
    elapsed: float,
    desired_period: float,
    enqueue_duration: float,
    queue_depth: int,
) -> float:
    """타겟 EPS를 뒤쳐질 때 경고 로그를 남긴다."""
    if elapsed < desired_period:
        return last_log_ts

    should_log = True
    if log_every_sec > 0:
        should_log = (loop_start - last_log_ts) >= log_every_sec
    if should_log:
        last_log_ts = loop_start
    logger.info(
        "[simulator] behind target service=%s target_eps=%.1f batch=%d "
        "duration=%.4fs target_interval=%.4fs enqueue=%.4fs queue=%d",
        service,
        target_eps,
        batch_size,
        elapsed,
        desired_period,
        enqueue_duration,
        queue_depth,
    )
    return last_log_ts
