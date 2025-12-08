# -----------------------------------------------------------------------------
# 파일명 : log_gateway/pipeline.py
# 목적   : 서비스별 배치 생성 루프 및 Kafka 퍼블리셔 태스크 구성
# 설명   : generator가 전달한 프로파일/시뮬레이터 기반으로 큐+워커 흐름을 실행
# -----------------------------------------------------------------------------
from __future__ import annotations

import asyncio
import random
import time
from typing import Any, Dict, List, Tuple

from .producer import publish, get_producer
from .config.stats import record_tps
from .config.timeband import current_hour_kst, pick_multiplier

# ===== 파이프라인(생성/전송) 파라미터 =====
# BATCH_MIN : int = 300
# BATCH_MAX : int = 600
LOG_BATCH_SIZE : int = 300
QUEUE_SIZE : int = 10000
PUBLISHER_WORKERS : int = 8
WORKER_BATCH_SIZE : int = 800

# # 퍼블리셔 튜닝(미니배치 드레인/폴링/백오프)
# WORKER_DRAIN_COUNT: int = int(os.getenv("LG_WORKER_DRAIN_COUNT", "5000"))
# WORKER_DRAIN_MS: int = int(os.getenv("LG_WORKER_DRAIN_MS", "5"))
# POLL_EVERY: int = int(os.getenv("LG_POLL_EVERY", "1000"))
# BUFFER_BACKOFF_MS: int = int(os.getenv("LG_BUFFER_BACKOFF_MS", "5"))


async def _service_stream_loop(
    service: str,
    simulator: Any,
    target_rps: float,
    publish_queue: "asyncio.Queue[Tuple[str, str, bool]]",
    bands: List[Any],
    weight_mode: str,
    # batch_range: Tuple[int, int],
    log_batch_size: int
) -> None:
    """서비스별로 배치 로그를 생성해 퍼블리시 큐에 쌓는다."""
    # batch_min, batch_max = batch_range  # 프로파일과 무관하게 고정된 배치 범위
    # batch_min = max(1, batch_min)
    # batch_max = max(batch_min, batch_max)

    while True:
        hour = current_hour_kst()  # 현재 시간대(KST) 결정
        multiplier = pick_multiplier(bands, hour_kst=hour, mode=weight_mode) if bands else 1.0  # 시간대 가중치 적용
        effective_rps = max(target_rps * multiplier, 0.01)  # 목표 RPS × multiplier
        # log_batch_size = random.randint(batch_min, batch_max)  # 배치 크기를 랜덤 선택
        log_batch_size = log_batch_size

        logs = simulator.generate_logs(log_batch_size)  # 시뮬레이터에서 로그 배치 생성
        
        # 2025-12-07 수정
        # 배치로 만든 로그를 다시 1건 씩 render + queue.put을 하는 이슈.
        # start = time.time()
        # for event in logs:
        #     payload = simulator.render(event)
        #     is_error = event.get("level") == "ERROR"
        #     record_tps(service)
        #     await publish_queue.put((service, payload, is_error))  # 큐에 (서비스, 페이로드, 에러여부) push
        # print("old_put_time", time.time() - start)

        # 배치 단위로 처리
        # start = time.time()

        payloads = [simulator.render(log) for log in logs] # 배치 단위 렌더링
        await asyncio.gather(*[                            # 배치 단위 큐 삽입
            publish_queue.put((service, payload, log.get("level") == "ERROR"))
            for payload, log in zip(payloads, logs)
        ])
        
        # print("new_put_time", time.time() - start)
        print("queue size:", publish_queue.qsize())
        
        sleep_time = log_batch_size / effective_rps  # 배치 처리에 소비해야 하는 시간 → 목표 RPS 맞추기 위함

        print("target_rps:", effective_rps, "batch:", log_batch_size, "sleep_time:", sleep_time)

        if sleep_time > 0:
            await asyncio.sleep(sleep_time)


async def _publisher_worker(
    worker_id: int,
    publish_queue: "asyncio.Queue[Tuple[str, str, bool]]",
    stats_queue: "asyncio.Queue[Tuple[str, int]]",
) -> None:
    """큐에 쌓인 로그를 Kafka에 발행"""
    
    # 병목 발생 : event 단위로 처리 원인
    # while True:
    #     service, payload, is_error = await publish_queue.get()
    #     try:
    #         await producer.publish(service, payload, replicate_error=is_error)  # Kafka publish (에러 토픽 복제 포함)
    #         # record_tps(service)  # kafka 발행 tps 측정
    #         stats_queue.put_nowait((service, 1))  # 통계 큐에 처리 건수 보고
    #     finally:
    #         publish_queue.task_done()
    while True:
        batch = []

        # 최소 1건
        batch.append(await publish_queue.get())

        # WORKER_BATCH_SIZE-1개 추가 drain
        for _ in range(WORKER_BATCH_SIZE - 1):
            try:
                batch.append(publish_queue.get_nowait())
            except asyncio.QueueEmpty:
                break

        # 병렬 publish
        await asyncio.gather(*[
            publish(service, payload, None, err)
            for (service, payload, err) in batch
        ])

        producer = get_producer()
        producer.poll(0)

        # --- 🔥 서비스별 카운트 집계 ---
        svc_counter = {}
        for (svc, _, _) in batch:
            svc_counter[svc] = svc_counter.get(svc, 0) + 1

        # --- 🔥 stats_queue에 서비스별로 push ---
        for svc, cnt in svc_counter.items():
            stats_queue.put_nowait((svc, cnt))

        # --- task_done 처리 ---
        for _ in batch:
            publish_queue.task_done()


def start_pipeline(
    simulators: Dict[str, Any],
    base_rps: float,
    bands: List[Any],
    service_rps: Dict[str, float],
    weight_mode: str,
) -> Tuple[
    "asyncio.Queue[Tuple[str, str, bool]]",
    "asyncio.Queue[Tuple[str, int]]",
    List[asyncio.Task],
    List[asyncio.Task],
]:
    """큐/워커 태스크를 초기화하고 반환."""
    # batch_range = (BATCH_MIN, BATCH_MAX)
    log_batch_size = LOG_BATCH_SIZE
    publish_queue: "asyncio.Queue[Tuple[str, str, bool]]" = asyncio.Queue(maxsize=QUEUE_SIZE)  # Kafka 전송 대기 큐
    stats_queue: "asyncio.Queue[Tuple[str, int]]" = asyncio.Queue()  # RPS 계산용 큐

    available_services = list(simulators.keys())
    service_count = max(len(available_services), 1)
    fallback_rps = base_rps / service_count  # mix에 없는 서비스 대비 기본 RPS

    service_tasks = [
        asyncio.create_task(
            _service_stream_loop(
                service=service,
                simulator=simulators[service],
                target_rps=service_rps.get(service, fallback_rps),
                publish_queue=publish_queue,
                bands=bands,
                weight_mode=weight_mode,
                # batch_range=batch_range,
                log_batch_size=log_batch_size,
            ),
            name=f"service-loop-{service}",
        )
        for service in available_services
    ]

    publisher_tasks = [
        asyncio.create_task(
            _publisher_worker(worker_id=i, publish_queue=publish_queue, stats_queue=stats_queue),
            name=f"publisher-{i}",
        )
        for i in range(PUBLISHER_WORKERS)
    ]

    return publish_queue, stats_queue, service_tasks, publisher_tasks
