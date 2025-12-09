# -----------------------------------------------------------------------------
# 파일명 : log_gateway/kafka_pipeline.py
# 목적   : 시뮬레이터 큐에서 로그를 읽어 Kafka에 전송하는 워커를 구성
# -----------------------------------------------------------------------------

from __future__ import annotations

import asyncio
from collections import Counter
from typing import List, Tuple

from .producer import BatchMessage, get_producer, publish_batch

# 퍼블리셔 기본 설정
PUBLISHER_WORKERS: int = 8
WORKER_BATCH_SIZE: int = 800
POLL_EVERY: int = 50


async def _publisher_worker(
    worker_id: int,
    publish_queue: "asyncio.Queue[Tuple[str, str, bool]]",
    stats_queue: "asyncio.Queue[Tuple[str, int]]",
) -> None:
    """큐에 쌓인 로그를 Kafka에 발행."""
    producer = get_producer()

    while True:
        batch = []

        batch.append(await publish_queue.get())
        for _ in range(WORKER_BATCH_SIZE - 1):
            try:
                batch.append(publish_queue.get_nowait())
            except asyncio.QueueEmpty:
                break

        messages = [
            BatchMessage(service, payload, None, err) for (service, payload, err) in batch
        ]
        await publish_batch(messages)

        processed = 0
        for _ in batch:
            processed += 1
            if processed % POLL_EVERY == 0:
                producer.poll(0)
        producer.poll(0)

        svc_counter = Counter(svc for svc, _, _ in batch)
        for svc, cnt in svc_counter.items():
            stats_queue.put_nowait((svc, cnt))

        for _ in batch:
            publish_queue.task_done()


def create_publisher_tasks(
    publish_queue: "asyncio.Queue[Tuple[str, str, bool]]",
    stats_queue: "asyncio.Queue[Tuple[str, int]]",
    worker_count: int = PUBLISHER_WORKERS,
) -> List[asyncio.Task]:
    """Kafka 퍼블리셔 워커 태스크 생성."""
    return [
        asyncio.create_task(
            _publisher_worker(worker_id=i, publish_queue=publish_queue, stats_queue=stats_queue),
            name=f"publisher-{i}",
        )
        for i in range(worker_count)
    ]
