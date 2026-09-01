# -----------------------------------------------------------------------------
# 파일명 : log_simulator/simulator/stream_pipeline.py
# 목적   : 서비스별 시뮬레이터 배치 생성 루프를 구성하고 큐에 로그를 적재
# -----------------------------------------------------------------------------

from __future__ import annotations

import asyncio
import logging
import time
import random
import math
from typing import Any

from .settings import get_simulator_settings
from ..models.messages import BatchMessage
from .stream_helpers import (
    apply_queue_backpressure,
    build_batch_messages_from_simulator,
    log_behind,
)

_logger = logging.getLogger("log_simulator.simulator.stream_pipeline")


async def run_simulator_loop(
    service: str,
    simulator: Any,
    target_eps: float,
    publish_queue: "asyncio.Queue[list[BatchMessage]]",
) -> None:
    """서비스별로 배치 로그를 생성해 퍼블리시 큐에 쌓는다."""
    settings = get_simulator_settings()
    throttle_scale = settings.queue_soft_scale_max
    target_interval_sec = max(settings.target_interval_sec, 0.01)
    base_batch_size = max(int(settings.log_batch_size), 1)
    auto_batch_size = int(math.ceil(target_eps * target_interval_sec))
    max_batch_size = max(base_batch_size, max(auto_batch_size, 1))
    carry = 0.0
    prev_ts = time.perf_counter()
    behind_log_every_sec = settings.behind_log_every_sec
    last_behind_log_ts = 0.0

    # 여러 루프가 같은 타이밍에 쏟아내는 걸 방지하기 위해 start jitter를 준다.
    await asyncio.sleep(random.uniform(0.0, target_interval_sec))

    while True:
        loop_start = time.perf_counter()
        now_ts = loop_start
        dt_actual = max(0.0, now_ts - prev_ts)
        prev_ts = now_ts

        effective_eps = max(target_eps, 0.01)
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
            enqueue_start = time.perf_counter()
            batch_items = await asyncio.to_thread(
                build_batch_messages_from_simulator,
                simulator,
                service,
                batch_size,
            )

            if batch_items:
                await publish_queue.put(batch_items)
                enqueue_duration = time.perf_counter() - enqueue_start

        desired_period = target_interval_sec
        elapsed = time.perf_counter() - loop_start
        sleep_time = desired_period - elapsed
        if sleep_time < 0:
            # 뒤쳐진 상태에서 0-sleep 스핀을 하면 CPU를 태우면서 더 불안정해진다.
            sleep_time = 0.001

        queue_depth = publish_queue.qsize()
        queue_capacity = publish_queue.maxsize

        last_behind_log_ts = log_behind(
            _logger,
            service=service,
            effective_eps=effective_eps,
            batch_size=batch_size,
            elapsed=elapsed,
            desired_period=desired_period,
            enqueue_duration=enqueue_duration,
            queue_depth=queue_depth,
            loop_start=loop_start,
            last_behind_log_ts=last_behind_log_ts,
            behind_log_every_sec=behind_log_every_sec,
        )

        throttle_scale, sleep_time, throttled = await apply_queue_backpressure(
            _logger,
            settings=settings,
            service=service,
            publish_queue=publish_queue,
            throttle_scale=throttle_scale,
            sleep_time=sleep_time,
            queue_depth=queue_depth,
            queue_capacity=queue_capacity,
        )
        if throttled:
            continue

        await asyncio.sleep(max(0.0, sleep_time))
