# -----------------------------------------------------------------------------
# 파일명 : log_simulator/generator/generator_pipeline.py
# 목적   : 서비스별 시뮬레이터 배치 생성 루프를 구성하고 큐에 로그를 적재
# -----------------------------------------------------------------------------

from __future__ import annotations

import asyncio
import logging
import time
import random
import math
from typing import Any, Dict, List, Tuple

from ..config.timeband import current_hour_kst, pick_multiplier
from ..config.settings import SIMULATOR_SETTINGS
from ..queue.metrics_keys import (
    ENQUEUED_TOTAL,
    GENERATED_TOTAL,
    OVERFLOW_DROPPED_TOTAL,
    QUEUE_DEPTH,
)
from ..queue.metrics_sink import emit_metric
from ..queue.queue_adapter import enqueue_batch, queue_maxsize, queue_qsize
from .batch_builder import apply_created_ms, build_batch_items

# 서비스 루프 기본 설정
LOG_BATCH_SIZE = SIMULATOR_SETTINGS.log_batch_size
# tick 기반으로 batch_size를 계산해 버스트를 줄인다. (초)
TICK_SEC = SIMULATOR_SETTINGS.tick_sec
# publish_queue는 "개별 로그"가 아니라 "배치(list)"를 담는다. (큐 연산 오버헤드 절감)
QUEUE_SIZE = SIMULATOR_SETTINGS.queue_size
LOOPS_PER_SERVICE = SIMULATOR_SETTINGS.loops_per_service
QUEUE_WARN_RATIO = SIMULATOR_SETTINGS.queue_warn_ratio
QUEUE_LOW_WATERMARK_RATIO = SIMULATOR_SETTINGS.queue_low_watermark_ratio
QUEUE_LOW_SLEEP_SCALE = SIMULATOR_SETTINGS.queue_low_sleep_scale
QUEUE_THROTTLE_RATIO = SIMULATOR_SETTINGS.queue_throttle_ratio
QUEUE_RESUME_RATIO = SIMULATOR_SETTINGS.queue_resume_ratio
QUEUE_THROTTLE_SLEEP = SIMULATOR_SETTINGS.queue_throttle_sleep
QUEUE_SOFT_THROTTLE_RATIO = SIMULATOR_SETTINGS.queue_soft_throttle_ratio
QUEUE_SOFT_RESUME_RATIO = SIMULATOR_SETTINGS.queue_soft_resume_ratio
QUEUE_SOFT_SCALE_STEP = SIMULATOR_SETTINGS.queue_soft_scale_step
QUEUE_SOFT_SCALE_MIN = SIMULATOR_SETTINGS.queue_soft_scale_min
QUEUE_SOFT_SCALE_MAX = SIMULATOR_SETTINGS.queue_soft_scale_max
SIM_BEHIND_LOG_EVERY_SEC = SIMULATOR_SETTINGS.behind_log_every_sec
OVERFLOW_POLICY = SIMULATOR_SETTINGS.overflow_policy
QUEUE_METRIC_INTERVAL_SEC = SIMULATOR_SETTINGS.queue_metric_interval_sec


_logger = logging.getLogger("log_simulator.generator_pipeline")
_logger.setLevel(logging.INFO)
if not _logger.handlers:
    handler = logging.StreamHandler()
    handler.setFormatter(
        logging.Formatter("%(asctime)s [%(levelname)s] %(name)s - %(message)s")
    )
    _logger.addHandler(handler)


async def _service_stream_loop(
    service: str,
    simulator: Any,
    target_eps: float,
    publish_queue: Any,
    bands: List[Any],
    weight_mode: str,
    log_batch_size: int,
    metrics_queue: Any | None = None,
    overflow_policy: str = OVERFLOW_POLICY,
    stop_event: Any | None = None,
) -> None:
    """서비스별로 배치 로그를 생성해 퍼블리시 큐에 쌓는다."""
    throttle_scale = QUEUE_SOFT_SCALE_MAX
    tick_sec = max(TICK_SEC, 0.01)
    max_batch_size = max(log_batch_size, 1)
    carry = 0.0
    prev_ts = time.perf_counter()
    behind_log_every_sec = SIM_BEHIND_LOG_EVERY_SEC
    last_behind_log_ts = 0.0
    last_queue_metric_ts = 0.0

    # 여러 루프가 같은 타이밍에 쏟아내는 걸 방지하기 위해 start jitter를 준다.
    await asyncio.sleep(random.uniform(0.0, tick_sec))

    while True:
        if stop_event is not None and stop_event.is_set():
            break
        loop_start = time.perf_counter()
        now_ts = loop_start
        dt_actual = max(0.0, now_ts - prev_ts)
        prev_ts = now_ts
        hour = current_hour_kst()

        multiplier = pick_multiplier(bands, hour_kst=hour, mode=weight_mode) if bands else 1.0
        effective_eps = _compute_effective_eps(simulator, target_eps, multiplier)
        scaled_eps = max(effective_eps * throttle_scale, 0.01)

        # 실제 경과시간 기반 토큰 버킷으로 평균 EPS를 맞춘다.
        carry += scaled_eps * dt_actual
        if carry > max_batch_size:
            carry = float(max_batch_size)
        batch_size = int(math.floor(carry))
        if batch_size > max_batch_size:
            batch_size = max_batch_size
        carry -= batch_size

        enqueue_duration = 0.0
        if batch_size > 0:
            events = simulator.generate_events(batch_size)  # batch_size = 요청 수
            created_ms = int(time.time() * 1000)
            apply_created_ms(events, created_ms)

            enqueue_start = time.perf_counter()
            enqueued_ms = int(time.time() * 1000)
            batch_items = build_batch_items(service, simulator, events, enqueued_ms)

            if batch_items:
                emit_metric(metrics_queue, GENERATED_TOTAL, len(batch_items), service)
                enqueued, dropped = enqueue_batch(
                    publish_queue=publish_queue,
                    batch_items=batch_items,
                    overflow_policy=overflow_policy,
                    fallback_maxsize=QUEUE_SIZE,
                )
                if enqueued:
                    emit_metric(metrics_queue, ENQUEUED_TOTAL, enqueued, service)
                if dropped:
                    emit_metric(metrics_queue, OVERFLOW_DROPPED_TOTAL, dropped, service)
                enqueue_duration = time.perf_counter() - enqueue_start

        desired_period = tick_sec
        elapsed = time.perf_counter() - loop_start
        sleep_time = desired_period - elapsed
        if sleep_time < 0:
            # 뒤쳐진 상태에서 0-sleep 스핀을 하면 CPU를 태우면서 더 불안정해진다.
            sleep_time = 0.001

        queue_depth = queue_qsize(publish_queue)
        queue_capacity = queue_maxsize(publish_queue, fallback=QUEUE_SIZE)

        if QUEUE_METRIC_INTERVAL_SEC > 0:
            now_metric_ts = time.perf_counter()
            if (now_metric_ts - last_queue_metric_ts) >= QUEUE_METRIC_INTERVAL_SEC:
                emit_metric(metrics_queue, QUEUE_DEPTH, queue_depth, None)
                last_queue_metric_ts = now_metric_ts

        if elapsed >= desired_period:
            should_log = True
            if behind_log_every_sec > 0:
                should_log = (loop_start - last_behind_log_ts) >= behind_log_every_sec
            if should_log:
                last_behind_log_ts = loop_start
            _logger.info(
                "[simulator] behind target service=%s target_eps=%.1f batch=%d "
                "duration=%.4fs target_interval=%.4fs enqueue=%.4fs queue=%d",
                service,
                effective_eps,
                batch_size,
                elapsed,
                desired_period,
                enqueue_duration,
                queue_depth,
            )

        if queue_capacity and queue_capacity > 0:
            fill_ratio = queue_depth / queue_capacity
            if fill_ratio >= QUEUE_THROTTLE_RATIO:
                throttle_scale = QUEUE_SOFT_SCALE_MIN
                throttle_started_at = time.perf_counter()
                # 큐 포화 시 강한 스로틀로 급격한 버스트를 막는다.
                _logger.info(
                    "[simulator] throttling service=%s queue=%d/%d (%.0f%%)",
                    service,
                    queue_depth,
                    queue_capacity,
                    fill_ratio * 100,
                )

                while True:
                    if stop_event is not None and stop_event.is_set():
                        return
                    await asyncio.sleep(QUEUE_THROTTLE_SLEEP)
                    queue_depth = queue_qsize(publish_queue)
                    fill_ratio = queue_depth / queue_capacity
                    if fill_ratio <= QUEUE_RESUME_RATIO:
                        _logger.info(
                            "[simulator] throttle release service=%s queue=%d/%d (%.0f%%)",
                            service,
                            queue_depth,
                            queue_capacity,
                            fill_ratio * 100,
                        )
                        throttle_duration = time.perf_counter() - throttle_started_at
                        _logger.info(
                            "[simulator] throttle duration service=%s duration=%.3fs",
                            service,
                            throttle_duration,
                        )
                        break
                continue
            if fill_ratio >= QUEUE_SOFT_THROTTLE_RATIO:
                new_scale = max(QUEUE_SOFT_SCALE_MIN, throttle_scale - QUEUE_SOFT_SCALE_STEP)
                if new_scale < throttle_scale:
                    # 큐가 차오르면 생성 속도를 점진적으로 낮춘다.
                    throttle_scale = new_scale
                    _logger.info(
                        "[simulator] soft throttle service=%s scale=%.2f queue=%d/%d (%.0f%%)",
                        service,
                        throttle_scale,
                        queue_depth,
                        queue_capacity,
                        fill_ratio * 100,
                    )
            elif fill_ratio <= QUEUE_SOFT_RESUME_RATIO:
                new_scale = min(QUEUE_SOFT_SCALE_MAX, throttle_scale + QUEUE_SOFT_SCALE_STEP)
                if new_scale > throttle_scale:
                    throttle_scale = new_scale
                    _logger.info(
                        "[simulator] soft throttle release service=%s scale=%.2f queue=%d/%d (%.0f%%)",
                        service,
                        throttle_scale,
                        queue_depth,
                        queue_capacity,
                        fill_ratio * 100,
                    )
            if sleep_time > 0 and fill_ratio <= QUEUE_LOW_WATERMARK_RATIO:
                # 큐가 비어있을 땐 sleep을 줄여 목표 EPS를 더 잘 맞춘다.
                sleep_time *= QUEUE_LOW_SLEEP_SCALE
            if fill_ratio >= QUEUE_WARN_RATIO:
                _logger.info(
                    "[simulator] queue backlog service=%s queue=%d/%d (%.0f%%)",
                    service,
                    queue_depth,
                    queue_capacity,
                    fill_ratio * 100,
                )

        await asyncio.sleep(max(0.0, sleep_time))


def _compute_effective_eps(simulator: Any, target_eps: float, multiplier: float) -> float:
    effective_eps = max(target_eps * multiplier, 0.01)
    # NOTE: event_mode 기반 보정(domain/http rate)은 비활성화.
    # mode = getattr(simulator, "event_mode", "all")
    # if mode == "domain":
    #     domain_rate = float(getattr(simulator, "domain_event_rate", 1.0))
    #     if domain_rate <= 0:
    #         domain_rate = 1.0
    #     effective_eps = max(effective_eps / max(domain_rate, 0.01), 0.01)
    # elif mode == "http":
    #     http_rate = float(getattr(simulator, "http_event_rate", 1.0))
    #     if http_rate <= 0:
    #         http_rate = 1.0
    #     effective_eps = max(effective_eps / max(http_rate, 0.01), 0.01)
    return effective_eps


def create_service_tasks(
    simulators: Dict[str, Any],
    base_eps: float,
    service_eps: Dict[str, float],
    bands: List[Any],
    weight_mode: str,
    log_batch_size: int = LOG_BATCH_SIZE,
    queue_size: int = QUEUE_SIZE,
    loops_per_service: int = LOOPS_PER_SERVICE,
    publish_queue: Any | None = None,
    metrics_queue: Any | None = None,
    overflow_policy: str = OVERFLOW_POLICY,
    stop_event: Any | None = None,
) -> Tuple[Any, List[asyncio.Task]]:
    """시뮬레이터 태스크들을 생성하고 publish 큐와 함께 반환."""
    if publish_queue is None:
        publish_queue = asyncio.Queue(maxsize=queue_size)

    available_services = list(simulators.keys())
    service_count = max(len(available_services), 1)
    fallback_eps = base_eps / service_count

    loops = max(loops_per_service, 1)
    service_tasks: List[asyncio.Task] = []
    for service in available_services:
        target = service_eps.get(service, fallback_eps)
        per_loop_eps = target / loops
        for idx in range(loops):
            task = asyncio.create_task(
                _service_stream_loop(
                    service=service,
                    simulator=simulators[service],
                    target_eps=per_loop_eps,
                    publish_queue=publish_queue,
                    bands=bands,
                    weight_mode=weight_mode,
                    log_batch_size=log_batch_size,
                    metrics_queue=metrics_queue,
                    overflow_policy=overflow_policy,
                    stop_event=stop_event,
                ),
                name=f"service-loop-{service}-{idx}",
            )
            service_tasks.append(task)

    return publish_queue, service_tasks
