# -----------------------------------------------------------------------------
# 파일명 : log_simulator/publisher/producer.py
# 목적   : confluent-kafka Producer 래퍼(단일 publish_batch_direct 경로)
# 설명   : bootstrap/batching/idempotence 설정과 서비스명→토픽 맵 제공, poll 책임을 단일화
# -----------------------------------------------------------------------------

from __future__ import annotations

import asyncio
import json
import time
from typing import Any, Dict, Optional, Sequence

from confluent_kafka import Producer

try:
    import orjson
except ImportError:  # pragma: no cover - optional dependency
    orjson = None

from .settings import PRODUCER_SETTINGS
from ..models.messages import BatchMessage
from .topic import get_topic

import logging
logger = logging.getLogger("log_simulator.producer")
logger.setLevel(logging.INFO)

def _ensure_logger_handler() -> None:
    """프로듀서 로거에 기본 핸들러를 연결한다."""
    if logger.handlers:
        return
    handler = logging.StreamHandler()
    handler.setFormatter(
        logging.Formatter("%(asctime)s [%(levelname)s] %(name)s - %(message)s")
    )
    logger.addHandler(handler)

_ensure_logger_handler()


def _build_producer_config() -> Dict[str, Any]:
    """프로듀서 설정 값을 딕셔너리로 구성한다."""
    s = PRODUCER_SETTINGS
    return {
        "bootstrap.servers": s.brokers,
        "client.id": s.client_id,
        "enable.idempotence": s.enable_idempotence,
        "acks": s.acks,
        "max.in.flight.requests.per.connection": 5,
        "compression.type": s.compression_type,
        "linger.ms": s.linger_ms,
        "batch.size": s.batch_size,
        "queue.buffering.max.kbytes": s.queue_buffering_max_kbytes,
        "queue.buffering.max.messages": s.queue_buffering_max_messages,
        "partitioner": "murmur2_random",
    }


_producer: Optional[Producer] = None

def get_producer() -> Producer:
    """모듈 전역에서 재사용할 Producer 인스턴스를 반환한다."""
    global _producer
    if _producer is None:
        _producer = Producer(_build_producer_config())
    return _producer

def close_producer(timeout: float = 5.0) -> None:
    """프로듀서를 flush 후 정리한다."""
    global _producer
    if _producer is None:
        return
    try:
        _producer.flush(timeout)
    except Exception:
        logger.exception("producer flush failed")
    finally:
        _producer = None


# ---------------------------------------------------------------------------
# 동기 발행 함수 (실제 Kafka I/O)
# ---------------------------------------------------------------------------

def _to_bytes(value: Optional[bytes | str | dict]) -> Optional[bytes]:
    """value/key를 bytes로 정규화해 producer에 전달한다."""
    if value is None:
        return None
    if isinstance(value, bytes):
        return value
    if isinstance(value, str):
        return value.encode("utf-8")
    if orjson is not None:
        return orjson.dumps(value)
    return json.dumps(value, ensure_ascii=False).encode("utf-8")

def _deliver(
    producer: Producer,
    service: str,
    value: bytes | str | dict,
    key: Optional[bytes | str] = None,
    replicate_error: bool = False,
) -> None:
    """단일 메시지를 Kafka로 전송하고 필요 시 에러 토픽도 복제한다."""
    topic = get_topic(service)

    def _delivery_report(err, msg):
        """Kafka 전송 결과 콜백을 기록한다."""
        if err is not None:
            logger.warning(
                "Kafka 전송 실패: topic=%s key=%s error=%s",
                msg.topic(),
                msg.key(),
                err,
            )

    encoded_value = _to_bytes(value)
    encoded_key = _to_bytes(key)

    producer.produce(
        topic=topic,
        value=encoded_value,
        key=encoded_key,
        callback=_delivery_report,
    )
    if replicate_error and service != "error":
        err_topic = get_topic("error")
        producer.produce(
            topic=err_topic,
            value=encoded_value,
            key=encoded_key,
            callback=_delivery_report,
        )


async def publish_batch_direct(
    batch: Sequence[BatchMessage],
    *,
    poll_every: int = 1000,
    backoff_sec: float = 0.001,
) -> None:
    """이벤트 루프에서 배치를 동기 enqueue하고 poll로 콜백을 처리한다."""
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
            except Exception:
                logger.exception(
                    "Kafka produce error: service=%s",
                    message.service,
                )
                raise

        if idx % poll_every == 0:
            producer.poll(0)

    producer.poll(0)


def publish_batch_direct_sync(
    batch: Sequence[BatchMessage],
    *,
    poll_every: int = 1000,
    backoff_sec: float = 0.001,
) -> None:
    """프로세스/스레드 기반 퍼블리셔에서 배치를 동기 발행한다."""
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
                    time.sleep(backoff_sec)
            except Exception:
                logger.exception(
                    "Kafka produce error: service=%s",
                    message.service,
                )
                raise

        if idx % poll_every == 0:
            producer.poll(0)

    producer.poll(0)
