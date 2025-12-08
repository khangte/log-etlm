# -----------------------------------------------------------------------------
# 파일명 : log_gateway/producer.py
# 목적   : confluent-kafka Producer를 감싸 FastAPI/generator가 동일한 publish() 래퍼로 Kafka에 전송
# 설명   : bootstrap/batching/idempotence 설정과 서비스명→토픽 맵, asyncio-friendly publish() 제공
# -----------------------------------------------------------------------------

from __future__ import annotations

import os
import asyncio
import atexit
from dataclasses import dataclass
from typing import Any, Dict, Optional, Sequence

from confluent_kafka import Producer

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

from .kafka_settings import ProducerSettings
SETTINGS = ProducerSettings()

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


# -----------------------------------------------------------------------------
# 내부 유틸리티 함수
# -----------------------------------------------------------------------------

def _build_producer_config() -> Dict[str, Any]:
    """
    Kafka Producer 생성에 필요한 기본 설정을 반환한다.
    - 멱등성/압축/배치/acks 등 안정성/성능 옵션 포함.

    Returns:
        Dict[str, Any]: confluent-kafka/python-kafka 등에서 사용할 설정 맵
    """
    config = {
        "bootstrap.servers": SETTINGS.brokers,
        "client.id": SETTINGS.client_id,
        # 안정성/성능 옵션(라이브러리에 따라 키가 다를 수 있음)
        "enable.idempotence": True,
        "compression.type": "zstd",
        "acks": "all",
        # 배치/지연
        "linger.ms": 5,
        "batch.num.messages": 10000,
    }
    if SETTINGS.security:
        config.update(SETTINGS.security)
    return config


_producer: Optional[Producer] = None

def get_producer() -> Producer:
    """
    모듈 전역에서 재사용할 단일 Producer 인스턴스를 반환한다.
    """
    global _producer
    if _producer is None:
        _producer = Producer(_build_producer_config())
    return _producer

def get_topic(service: str) -> str:
    """
    서비스 이름 → Kafka 토픽 문자열로 매핑.
    매핑에 없으면 logs.{service} 로 fallback.
    """
    return SETTINGS.topics.get(service, f"logs.{service}")


# ---------------------------------------------------------------------------
# 동기 발행 함수 (실제 Kafka I/O)
# ---------------------------------------------------------------------------

def publish_sync(service: str, value: str, key: str | None = None, replicate_error: bool = False) -> None:
    """
    실제 Kafka로 보내는 동기 함수.
    asyncio 환경에서는 직접 호출하지 말고 publish() 를 await 할 것.
    """
    global _msg_count
    producer = get_producer()
    topic = get_topic(service)

    # logger.info("Publishing to Kafka topic=%s service=%s", topic, service)

    def _delivery_report(err, msg):
        if err is not None:
            logger.warning(
                "Kafka publish failed: topic=%s key=%s error=%s",
                msg.topic(),
                msg.key(),
                err,
            )

    encoded_key = key.encode("utf-8") if key else None
    encoded_value = value.encode("utf-8")

    producer.produce(
        topic=topic,
        key=encoded_key,
        value=encoded_value,
        callback=_delivery_report,
    )
    if replicate_error and service != "error":
        producer.produce(
            topic=get_topic("error"),
            key=encoded_key,
            value=encoded_value,
            callback=_delivery_report,
        )


async def publish(service: str, value: str, key: str | None = None, replicate_error: bool = False) -> None:
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
    for message in batch:
        publish_sync(message.service, message.value, message.key, message.replicate_error)

async def publish_batch(batch: Sequence[BatchMessage]) -> None:
    """배치 발행을 한 번의 executor 작업으로 실행."""
    loop = asyncio.get_running_loop()
    executor = get_executor()
    await loop.run_in_executor(executor, publish_batch_sync, batch)
