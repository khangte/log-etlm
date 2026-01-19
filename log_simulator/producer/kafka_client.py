# -----------------------------------------------------------------------------
# 파일명 : log_simulator/producer/kafka_client.py
# 목적   : confluent-kafka Producer 래퍼(단일 publish_batch_direct 경로)
# 설명   : bootstrap/batching/idempotence 설정과 서비스명→토픽 맵 제공, poll 책임을 단일화
# -----------------------------------------------------------------------------

from __future__ import annotations

import asyncio
import logging
from typing import Any, Dict, Optional, Sequence

from confluent_kafka import Producer

from ..models.messages import BatchMessage
from .settings import ProducerSettings, get_producer_settings
from .topic import get_topic

logger = logging.getLogger("log_simulator.producer")
logger.setLevel(logging.INFO)


def _ensure_logger_handler() -> None:
    """ensure_logger_handler 처리를 수행한다."""
    if logger.handlers:
        return
    handler = logging.StreamHandler()
    handler.setFormatter(
        logging.Formatter("%(asctime)s [%(levelname)s] %(name)s - %(message)s")
    )
    logger.addHandler(handler)

_ensure_logger_handler()


def build_producer_config(settings: ProducerSettings | None = None) -> Dict[str, Any]:
    """build_producer_config 처리를 수행한다."""
    s = settings or get_producer_settings()
    return s.to_kafka_config()


class KafkaProducerClient:
    """Kafka 프로듀서 클라이언트를 관리한다."""
    def __init__(self, settings: ProducerSettings | None = None) -> None:
        self._settings = settings or get_producer_settings()
        self._producer: Optional[Producer] = None

    def _ensure_producer(self) -> Producer:
        """프로듀서 인스턴스를 준비한다."""
        if self._producer is None:
            self._producer = Producer(build_producer_config(self._settings))
        return self._producer

    def get_producer(self) -> Producer:
        """인스턴스 단위로 Producer를 반환한다."""
        return self._ensure_producer()

    def close(self, timeout: float = 5.0) -> None:
        """프로듀서를 flush 후 정리한다."""
        if self._producer is None:
            return
        try:
            self._producer.flush(timeout)
        except Exception:
            logger.exception("producer flush failed")
        finally:
            self._producer = None

    async def publish_batch(
        self,
        batch: Sequence[BatchMessage],
        *,
        poll_every: int = 1000,
        backoff_sec: float = 0.001,
    ) -> None:
        """
        threadpool 없이 event loop에서 바로 produce한다.

        confluent_kafka.Producer.produce()는 비동기 enqueue이며 빠르지만,
        내부 버퍼가 가득 차면 BufferError가 날 수 있어 poll+backoff로 흡수한다.
        delivery callback 처리는 이 함수의 poll로만 보장한다.
        BufferError 외 예외는 호출부에서 처리할 수 있도록 상위로 전달한다.
        """
        producer = self._ensure_producer()
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


_client: Optional[KafkaProducerClient] = None


def get_client() -> KafkaProducerClient:
    """모듈 전역에서 재사용할 단일 ProducerClient를 반환한다."""
    global _client
    if _client is None:
        _client = KafkaProducerClient()
    return _client


def get_producer() -> Producer:
    """
    모듈 전역에서 재사용할 단일 Producer 인스턴스를 반환한다.
    """
    return get_client().get_producer()


def close_producer(timeout: float = 5.0) -> None:
    """프로듀서를 flush 후 정리한다."""
    global _client
    if _client is None:
        return
    _client.close(timeout)
    _client = None


# ---------------------------------------------------------------------------
# 동기 발행 함수 (실제 Kafka I/O)
# ---------------------------------------------------------------------------

def _to_bytes(value: Optional[bytes | str]) -> Optional[bytes]:
    # value/key를 bytes로 정규화해 producer에 바로 전달한다.
    """to_bytes 처리를 수행한다."""
    if value is None:
        return None
    if isinstance(value, bytes):
        return value
    return str(value).encode("utf-8")

def _deliver(
    producer: Producer,
    service: str,
    value: bytes | str,
    key: Optional[bytes | str] = None,
    replicate_error: bool = False,
) -> None:
    """deliver 처리를 수행한다."""
    topic = get_topic(service)

    def _delivery_report(err, msg):
        """delivery_report 처리를 수행한다."""
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
    """단일 클라이언트 인스턴스를 통해 batch를 발행한다."""
    await get_client().publish_batch(
        batch,
        poll_every=poll_every,
        backoff_sec=backoff_sec,
    )
