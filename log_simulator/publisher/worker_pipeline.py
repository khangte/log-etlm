# -----------------------------------------------------------------------------
# 파일명 : log_simulator/publisher/worker_pipeline.py
# 목적   : 시뮬레이터 큐에서 로그를 읽어 Kafka에 전송하는 워커를 구성
# -----------------------------------------------------------------------------

from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass
from typing import List, Tuple

from .dlq import DlqPublisher, get_dlq_publisher
from ..producer.kafka_client import KafkaProducerClient, get_client
from ..models.messages import BatchMessage
from .settings import PublisherSettings, get_publisher_settings
from .worker_helpers import (
    WorkerMetricsConfig,
    collect_batch,
    log_worker_metrics,
    push_stats,
)

_logger = logging.getLogger("log_simulator.publisher.worker_pipeline")
_logger.setLevel(logging.INFO)
if not _logger.handlers:
    handler = logging.StreamHandler()
    handler.setFormatter(
        logging.Formatter("%(asctime)s [%(levelname)s] %(name)s - %(message)s")
    )
    _logger.addHandler(handler)


def _build_metrics_config(settings: PublisherSettings) -> WorkerMetricsConfig:
    """메트릭 설정을 구성한다."""
    return WorkerMetricsConfig(
        idle_warn_sec=settings.idle_warn_sec,
        send_warn_sec=settings.send_warn_sec,
        queue_warn_ratio=settings.queue_warn_ratio,
    )


@dataclass(frozen=True)
class PublisherWorker:
    """퍼블리셔 워커 실행 단위를 담는다."""
    worker_id: int
    publish_queue: "asyncio.Queue[list[BatchMessage]]"
    stats_queue: "asyncio.Queue[Tuple[str, int]]"
    settings: PublisherSettings
    metrics_config: WorkerMetricsConfig
    producer: KafkaProducerClient
    dlq_publisher: DlqPublisher
    logger: logging.Logger

    async def run(self) -> None:
        """큐에 쌓인 로그를 Kafka에 발행."""
        while True:
            batch, consumed_batches, wait_duration = await collect_batch(
                self.publish_queue,
                self.settings.worker_batch_size,
            )

            send_ok = True
            send_start = time.perf_counter()
            try:
                await self.producer.publish_batch(batch)
            except Exception as exc:
                send_ok = False
                self.logger.exception(
                    "[publisher] send failed worker=%d batch=%d",
                    self.worker_id,
                    len(batch),
                )
                await self.dlq_publisher.publish_batch(batch, exc)
                if self.settings.retry_backoff_sec > 0:
                    await asyncio.sleep(self.settings.retry_backoff_sec)
            send_duration = time.perf_counter() - send_start

            queue_depth = self.publish_queue.qsize()
            queue_capacity = self.publish_queue.maxsize

            log_worker_metrics(
                self.logger,
                config=self.metrics_config,
                worker_id=self.worker_id,
                wait_duration=wait_duration,
                send_duration=send_duration,
                batch_size=len(batch),
                queue_depth=queue_depth,
                queue_capacity=queue_capacity,
            )

            # 성공한 배치만 stats에 반영해 오탐을 줄인다.
            if send_ok:
                push_stats(self.stats_queue, batch)

            for _ in range(consumed_batches):
                self.publish_queue.task_done()


def create_publisher_workers(
    publish_queue: "asyncio.Queue[list[BatchMessage]]",
    stats_queue: "asyncio.Queue[Tuple[str, int]]",
    worker_count: int | None = None,
    *,
    settings: PublisherSettings | None = None,
    producer: KafkaProducerClient | None = None,
    dlq_publisher: DlqPublisher | None = None,
) -> List[asyncio.Task]:
    """Kafka 퍼블리셔 워커 태스크 생성."""
    resolved_settings = settings or get_publisher_settings()
    resolved_metrics = _build_metrics_config(resolved_settings)
    resolved_producer = producer or get_client()
    resolved_dlq = dlq_publisher or get_dlq_publisher(resolved_producer)
    resolved_worker_count = (
        resolved_settings.workers if worker_count is None else worker_count
    )
    return [
        asyncio.create_task(
            PublisherWorker(
                worker_id=i,
                publish_queue=publish_queue,
                stats_queue=stats_queue,
                settings=resolved_settings,
                metrics_config=resolved_metrics,
                producer=resolved_producer,
                dlq_publisher=resolved_dlq,
                logger=_logger,
            ).run(),
            name=f"publisher-{i}",
        )
        for i in range(resolved_worker_count)
    ]
