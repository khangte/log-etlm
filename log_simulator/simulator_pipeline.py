# -----------------------------------------------------------------------------
# 파일명 : log_simulator/simulator_pipeline.py
# 목적   : 서비스별 시뮬레이터 배치 생성 루프를 구성하고 큐에 로그를 적재
# -----------------------------------------------------------------------------

from __future__ import annotations

import asyncio
import logging
import time
import random
import math
from typing import Any, Dict, List, Optional, Tuple

from .config.timeband import current_hour_kst, pick_multiplier
from .config.settings import SIMULATOR_SETTINGS
from .models.messages import BatchMessage

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


_logger = logging.getLogger("log_simulator.simulator_pipeline")
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
    publish_queue: "asyncio.Queue[list[BatchMessage]]",
    bands: List[Any],
    weight_mode: str,
    log_batch_size: int,
) -> None:
    """서비스별로 배치 로그를 생성해 퍼블리시 큐에 쌓는다."""
    throttle_scale = QUEUE_SOFT_SCALE_MAX
    tick_sec = max(TICK_SEC, 0.01)
    max_batch_size = max(log_batch_size, 1)
    carry = 0.0
    prev_ts = time.perf_counter()
    behind_log_every_sec = SIM_BEHIND_LOG_EVERY_SEC
    last_behind_log_ts = 0.0

    # 여러 루프가 같은 타이밍에 쏟아내는 걸 방지하기 위해 start jitter를 준다.
    await asyncio.sleep(random.uniform(0.0, tick_sec))

    while True:
        loop_start = time.perf_counter()
        now_ts = loop_start
        dt_actual = max(0.0, now_ts - prev_ts)
        prev_ts = now_ts
        hour = current_hour_kst()

        multiplier = pick_multiplier(bands, hour_kst=hour, mode=weight_mode) if bands else 1.0
        effective_eps = max(target_eps * multiplier, 0.01)
        mode = getattr(simulator, "event_mode", "all")
        if mode == "domain":
            domain_rate = float(getattr(simulator, "domain_event_rate", 1.0))
            if domain_rate <= 0:
                domain_rate = 1.0
            effective_eps = max(effective_eps / max(domain_rate, 0.01), 0.01)
        elif mode == "http":
            http_rate = float(getattr(simulator, "http_event_rate", 1.0))
            if http_rate <= 0:
                http_rate = 1.0
            effective_eps = max(effective_eps / max(http_rate, 0.01), 0.01)
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

            enqueue_start = time.perf_counter()
            batch_items: list[BatchMessage] = []

            # err 판정 함수(로컬로 두면 빠르고 깔끔)
            def is_err_event(ev: dict) -> bool:
                # 도메인 이벤트는 result가 있으면 그걸 우선
                r = ev.get("result")
                if r is not None:
                    return (r == "fail")
                # HTTP 이벤트는 status_code로 판정(기본 5xx)
                sc = ev.get("status_code")
                if isinstance(sc, int):
                    return sc >= 500
                return False

            for ev in events:
                payload = simulator.render_bytes(ev)
                request_id = ev.get("request_id")
                # request_id 기반 key로 파티션 분산/ordering 기본 보장.
                key = str(request_id).encode("utf-8") if request_id else None
                batch_items.append(
                    BatchMessage(
                        service=service,
                        value=payload,
                        key=key,
                        replicate_error=is_err_event(ev),
                    )
                )

            if batch_items:
                await publish_queue.put(batch_items)
                enqueue_duration = time.perf_counter() - enqueue_start

        desired_period = tick_sec
        elapsed = time.perf_counter() - loop_start
        sleep_time = desired_period - elapsed
        if sleep_time < 0:
            # 뒤쳐진 상태에서 0-sleep 스핀을 하면 CPU를 태우면서 더 불안정해진다.
            sleep_time = 0.001

        queue_depth = publish_queue.qsize()
        queue_capacity = publish_queue.maxsize

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
                    await asyncio.sleep(QUEUE_THROTTLE_SLEEP)
                    queue_depth = publish_queue.qsize()
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


def create_service_tasks(
    simulators: Dict[str, Any],
    base_eps: float,
    service_eps: Dict[str, float],
    bands: List[Any],
    weight_mode: str,
    log_batch_size: int = LOG_BATCH_SIZE,
    queue_size: int = QUEUE_SIZE,
    loops_per_service: int = LOOPS_PER_SERVICE,
) -> Tuple["asyncio.Queue[list[BatchMessage]]", List[asyncio.Task]]:
    """시뮬레이터 태스크들을 생성하고 publish 큐와 함께 반환."""
    publish_queue: "asyncio.Queue[list[BatchMessage]]" = asyncio.Queue(maxsize=queue_size)

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
                ),
                name=f"service-loop-{service}-{idx}",
            )
            service_tasks.append(task)

    return publish_queue, service_tasks
