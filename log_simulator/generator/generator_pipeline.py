# -----------------------------------------------------------------------------
# 파일명 : log_simulator/generator/generator_pipeline.py
# 목적   : 서비스별 시뮬레이터 배치 생성 루프를 구성하고 큐에 로그를 적재
# -----------------------------------------------------------------------------

from __future__ import annotations

import asyncio
import logging
import time
import random
from typing import Any, Dict, List, Tuple

from ..config.timeband import current_hour_kst, pick_multiplier
from .settings import SIMULATOR_SETTINGS
from ..queue.queue_adapter import queue_maxsize, queue_qsize
from .loop_helpers import (
    compute_batch_size,
    enqueue_batch_events,
    maybe_emit_queue_depth,
    maybe_log_behind_target,
)
from .queue_throttle import apply_queue_throttle

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
        effective_eps = max(target_eps * multiplier, 0.01)
        scaled_eps = max(effective_eps * throttle_scale, 0.01)

        # 실제 경과시간 기반 토큰 버킷으로 평균 EPS를 맞춘다.
        batch_size, carry = compute_batch_size(
            carry=carry,
            scaled_eps=scaled_eps,
            dt_actual=dt_actual,
            max_batch_size=max_batch_size,
        )
        enqueue_duration = enqueue_batch_events(
            service=service,
            simulator=simulator,
            batch_size=batch_size,
            publish_queue=publish_queue,
            metrics_queue=metrics_queue,
            overflow_policy=overflow_policy,
            queue_size=QUEUE_SIZE,
        )

        desired_period = tick_sec
        elapsed = time.perf_counter() - loop_start
        sleep_time = desired_period - elapsed
        if sleep_time < 0:
            # 뒤쳐진 상태에서 0-sleep 스핀을 하면 CPU를 태우면서 더 불안정해진다.
            sleep_time = 0.001

        queue_depth = queue_qsize(publish_queue)
        queue_capacity = queue_maxsize(publish_queue, fallback=QUEUE_SIZE)

        last_queue_metric_ts = maybe_emit_queue_depth(
            metrics_queue=metrics_queue,
            queue_depth=queue_depth,
            interval_sec=QUEUE_METRIC_INTERVAL_SEC,
            last_metric_ts=last_queue_metric_ts,
        )
        last_behind_log_ts = maybe_log_behind_target(
            logger=_logger,
            loop_start=loop_start,
            last_log_ts=last_behind_log_ts,
            log_every_sec=behind_log_every_sec,
            service=service,
            target_eps=effective_eps,
            batch_size=batch_size,
            elapsed=elapsed,
            desired_period=desired_period,
            enqueue_duration=enqueue_duration,
            queue_depth=queue_depth,
        )

        throttle_scale, sleep_time, throttle_action = await apply_queue_throttle(
            publish_queue=publish_queue,
            queue_depth=queue_depth,
            queue_capacity=queue_capacity,
            throttle_scale=throttle_scale,
            sleep_time=sleep_time,
            stop_event=stop_event,
            logger=_logger,
            service=service,
            queue_throttle_ratio=QUEUE_THROTTLE_RATIO,
            queue_resume_ratio=QUEUE_RESUME_RATIO,
            queue_throttle_sleep=QUEUE_THROTTLE_SLEEP,
            queue_soft_throttle_ratio=QUEUE_SOFT_THROTTLE_RATIO,
            queue_soft_resume_ratio=QUEUE_SOFT_RESUME_RATIO,
            queue_soft_scale_step=QUEUE_SOFT_SCALE_STEP,
            queue_soft_scale_min=QUEUE_SOFT_SCALE_MIN,
            queue_soft_scale_max=QUEUE_SOFT_SCALE_MAX,
            queue_low_watermark_ratio=QUEUE_LOW_WATERMARK_RATIO,
            queue_low_sleep_scale=QUEUE_LOW_SLEEP_SCALE,
            queue_warn_ratio=QUEUE_WARN_RATIO,
        )
        if throttle_action == "exit":
            return
        if throttle_action == "continue":
            continue

        await asyncio.sleep(max(0.0, sleep_time))


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
