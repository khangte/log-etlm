# -----------------------------------------------------------------------------
# 파일명 : log_gateway/producer.py
# 목적   : confluent-kafka Producer를 감싸 FastAPI/generator가 동일한 publish() 래퍼로 Kafka에 전송
# 설명   : bootstrap/batching/idempotence 설정과 서비스명→토픽 맵, asyncio-friendly publish() 제공
# -----------------------------------------------------------------------------

from __future__ import annotations

import os
import asyncio
import atexit
import threading
import time
from dataclasses import dataclass, field
from typing import Any, Dict, Optional, Sequence

from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient

from concurrent.futures import ThreadPoolExecutor
_EXECUTOR: Optional[ThreadPoolExecutor] = None
_EXECUTOR_SHUTDOWN_REGISTERED = False

def get_executor() -> ThreadPoolExecutor:
    """Lazy executor with shutdown hook."""
    global _EXECUTOR, _EXECUTOR_SHUTDOWN_REGISTERED
    if _EXECUTOR is None:
        max_workers = int(os.getenv("LG_PRODUCER_EXECUTOR", "32"))
        _EXECUTOR = ThreadPoolExecutor(max_workers=max_workers)
    if not _EXECUTOR_SHUTDOWN_REGISTERED:
        _EXECUTOR_SHUTDOWN_REGISTERED = True
        atexit.register(lambda: _EXECUTOR.shutdown(wait=False) if _EXECUTOR else None)
    return _EXECUTOR

import logging
logger = logging.getLogger("log_gateway.producer")
logger.setLevel(logging.INFO)

def _ensure_logger_handler() -> None:
    if logger.handlers:
        return
    handler = logging.StreamHandler()
    handler.setFormatter(
        logging.Formatter("%(asctime)s [%(levelname)s] %(name)s - %(message)s")
    )
    logger.addHandler(handler)

_ensure_logger_handler()


@dataclass
class ProducerSettings:
    brokers: str = os.getenv("KAFKA_BOOTSTRAP")
    client_id: str = os.getenv("KAFKA_CLIENT_ID")
    topics: Dict[str, str] = field(default_factory=lambda: {
        "auth":    "logs.auth",
        "order":   "logs.order",
        "payment": "logs.payment",
        "notify":  "logs.notify",
        "error":   "logs.error",
        # "dlq":  "dlq.logs",
    })
    security: Optional[Dict[str, Any]] = None

    def _build_producer_config(self) -> Dict[str, Any]:
        linger_ms = os.getenv("LG_PRODUCER_LINGER_MS", "5")
        batch_num_messages = os.getenv("LG_PRODUCER_BATCH_NUM_MESSAGES", "1000")
        queue_buffering_max_kbytes = int(os.getenv("LG_PRODUCER_QUEUE_MAX_KBYTES", str(128 * 1024)))
        queue_buffering_max_messages = os.getenv("LG_PRODUCER_QUEUE_MAX_MESSAGES", "500000")
        enable_idempotence = os.getenv("LG_PRODUCER_ENABLE_IDEMPOTENCE", "true").strip().lower() in (
            "1",
            "true",
            "yes",
            "y",
        )
        acks = os.getenv("LG_PRODUCER_ACKS", "all")
        compression_type = os.getenv("LG_PRODUCER_COMPRESSION", "snappy")
        config = {
            "bootstrap.servers": SETTINGS.brokers,
            "client.id": SETTINGS.client_id,

            # 안정성: 멱등성 유지
            "enable.idempotence": enable_idempotence,
            "acks": acks,
            "max.in.flight.requests.per.connection": 5,  # 약간 여유를 주되 at-least-once 보장

            # 압축 → 기본 snappy, 필요 시 env로 조정
            "compression.type": compression_type,

            # 배치/지연 → 처리량이 필요하면 linger/batch를 키운다.
            "linger.ms": linger_ms,
            "batch.num.messages": batch_num_messages,

            # 내부 큐: 극단적 버스트만 흡수 (64MB / 100k)
            "queue.buffering.max.kbytes": queue_buffering_max_kbytes,
            "queue.buffering.max.messages": queue_buffering_max_messages,

            # 파티션 분배 방식
            "partitioner": "murmur2_random",
        }
        if self.security:
            config.update(self.security)
        return config

SETTINGS = ProducerSettings()


POLL_THREAD_SLEEP_SEC = float(os.getenv("LG_PRODUCER_POLL_SLEEP_SEC", "0.01"))
_PRODUCER_POLL_THREAD: Optional[threading.Thread] = None
_PRODUCER_POLL_SHUTDOWN = threading.Event()

_producer: Optional[Producer] = None
_partition_counters: Dict[str, int] = {}
_topic_partitions: Dict[str, int] = {}


def _load_topic_partitions() -> None:
    global _topic_partitions
    admin = AdminClient({"bootstrap.servers": SETTINGS.brokers})
    md = admin.list_topics(timeout=5)

    for topic, desc in md.topics.items():
        if desc.error is None:
            _topic_partitions[topic] = len(desc.partitions)

def get_producer() -> Producer:
    """
    모듈 전역에서 재사용할 단일 Producer 인스턴스를 반환한다.
    """
    global _producer
    if _producer is None:
        _producer = Producer(SETTINGS._build_producer_config())
        _load_topic_partitions()
        _start_poll_thread()
    return _producer

def _select_partition(topic: str) -> int:
    count = _topic_partitions.get(topic, 1)
    if count <= 1:
        return 0

    idx = _partition_counters.get(topic, 0)
    next_idx = (idx + 1) % count
    _partition_counters[topic] = next_idx
    return next_idx

def _start_poll_thread() -> None:
    global _PRODUCER_POLL_THREAD
    if _PRODUCER_POLL_THREAD is not None:
        return

    def _poll_loop() -> None:
        while not _PRODUCER_POLL_SHUTDOWN.is_set():
            producer = _producer
            if producer is not None:
                producer.poll(0)
            time.sleep(POLL_THREAD_SLEEP_SEC)

    _PRODUCER_POLL_THREAD = threading.Thread(target=_poll_loop, name="lg-producer-poll", daemon=True)
    _PRODUCER_POLL_THREAD.start()

    def _stop_poll_thread() -> None:
        _PRODUCER_POLL_SHUTDOWN.set()

    atexit.register(_stop_poll_thread)

def get_topic(service: str) -> str:
    """
    서비스 이름 → Kafka 토픽 문자열로 매핑.
    매핑에 없으면 logs.{service} 로 fallback.
    """
    return SETTINGS.topics.get(service, f"logs.{service}")


# ---------------------------------------------------------------------------
# 동기 발행 함수 (실제 Kafka I/O)
# ---------------------------------------------------------------------------

def _deliver(
    producer: Producer,
    service: str,
    value: str,
    key: Optional[str] = None,
    replicate_error: bool = False,
) -> None:
    topic = get_topic(service)

    def _delivery_report(err, msg):
        if err is not None:
            logger.warning(
                "Kafka publish failed: topic=%s key=%s error=%s",
                msg.topic(),
                msg.key(),
                err,
            )

    encoded_value = value.encode("utf-8")
    encoded_key = None

    partition = _select_partition(topic)

    producer.produce(
        topic=topic,
        value=encoded_value,
        key=encoded_key,
        partition=partition,
        callback=_delivery_report,
    )
    if replicate_error and service != "error":
        err_topic = get_topic("error")
        err_partition = _select_partition(err_topic)
        producer.produce(
            topic=err_topic,
            value=encoded_value,
            key=encoded_key,
            partition=err_partition,
            callback=_delivery_report,
        )


def publish_sync(service: str, value: str, key: Optional[str] = None, replicate_error: bool = False) -> None:
    """
    실제 Kafka로 보내는 동기 함수.
    asyncio 환경에서는 직접 호출하지 말고 publish() 를 await 할 것.
    """
    producer = get_producer()
    _deliver(producer, service, value, key, replicate_error)


async def publish(service: str, value: str, key: Optional[str] = None, replicate_error: bool = False) -> None:
    """
    asyncio 환경에서 사용하는 비동기 발행 함수.

    내부에서는 publish_sync 를 thread pool 에서 실행하므로
    FastAPI 이벤트 루프를 막지 않는다.
    """
    loop = asyncio.get_running_loop()
    executor = get_executor()
    await loop.run_in_executor(executor, publish_sync, service, value, key, replicate_error)


@dataclass(frozen=True)
class BatchMessage:
    service: str
    value: str
    key: Optional[str]
    replicate_error: bool

def publish_batch_sync(batch: Sequence[BatchMessage]) -> None:
    """동일 스레드 풀 작업 내에서 배치를 순차 처리."""
    producer = get_producer()
    for message in batch:
        _deliver(producer, message.service, message.value, message.key, message.replicate_error)

async def publish_batch(batch: Sequence[BatchMessage]) -> None:
    """배치 발행을 한 번의 executor 작업으로 실행."""
    loop = asyncio.get_running_loop()
    executor = get_executor()
    await loop.run_in_executor(executor, publish_batch_sync, batch)


async def publish_batch_direct(
    batch: Sequence[BatchMessage],
    *,
    poll_every: int = 1000,
    backoff_sec: float = 0.001,
) -> None:
    """
    threadpool 없이 event loop에서 바로 produce한다.

    confluent_kafka.Producer.produce()는 비동기 enqueue이며 빠르지만,
    내부 버퍼가 가득 차면 BufferError가 날 수 있어 poll+backoff로 흡수한다.
    """
    producer = get_producer()
    backoff_sec = max(backoff_sec, 0.0)
    poll_every = max(int(poll_every), 1)

    for idx, message in enumerate(batch, start=1):
        while True:
            try:
                _deliver(
                    producer,
                    message.service,
                    message.value,
                    message.key,
                    message.replicate_error,
                )
                break
            except BufferError:
                producer.poll(0)
                if backoff_sec > 0:
                    await asyncio.sleep(backoff_sec)

        if idx % poll_every == 0:
            producer.poll(0)

    producer.poll(0)
