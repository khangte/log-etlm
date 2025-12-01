# -----------------------------------------------------------------------------
# 파일명 : log_gateway/producer.py
# 목적   : confluent-kafka Producer를 감싸 FastAPI/generator가 동일한 publish() 래퍼로 Kafka에 전송
# 설명   : bootstrap/batching/idempotence 설정과 서비스명→토픽 맵, asyncio-friendly publish() 제공
# -----------------------------------------------------------------------------

from __future__ import annotations

import asyncio
from typing import Any, Dict, Optional

from confluent_kafka import Producer

import logging
logger = logging.getLogger("log_gateway.producer")
logger.setLevel(logging.INFO)

if not logger.handlers:
    _handler = logging.StreamHandler()
    _handler.setFormatter(
        logging.Formatter("%(asctime)s [%(levelname)s] %(name)s - %(message)s")
    )
    logger.addHandler(_handler)

# ===== 브로커/클라이언트/토픽 =====
# 컨테이너 간 통신 기본값은 kafka:9092 (호스트에서 쓸 땐 localhost:29092 등으로 교체)
BROKERS: str = "kafka:9092"
CLIENT_ID: str = "log-forge-sim"   # 프로젝트명에 맞춤

# 서비스별 토픽 맵 + error/DLQ (DLQ는 선택)
TOPICS: Dict[str, str] = {
    "auth":    "logs.auth",
    "order":   "logs.order",
    "payment": "logs.payment",
    "notify":  "logs.notify",
    "error":   "logs.error",   # 에러 복제 발행용
    # "dlq":     "dlq.logs",     # 파싱 실패 등 사후 처리용(선택)
}

# (선택) 보안 설정. 기본은 None(로컬)
SECURITY: Optional[Dict[str, Any]] = None

def _build_producer_config() -> Dict[str, Any]:
    """
    Kafka Producer 생성에 필요한 기본 설정을 반환한다.
    - 멱등성/압축/배치/acks 등 안정성/성능 옵션 포함.

    Returns:
        Dict[str, Any]: confluent-kafka/python-kafka 등에서 사용할 설정 맵
    """
    config = {
        "bootstrap.servers": BROKERS,
        "client.id": CLIENT_ID,
        # 안정성/성능 옵션(라이브러리에 따라 키가 다를 수 있음)
        "enable.idempotence": True,
        "compression.type": "zstd",
        "acks": "all",
        # 배치/지연
        "linger.ms": 5,
        "batch.num.messages": 10000,
    }
    if SECURITY:
        config.update(SECURITY)
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
    return TOPICS.get(service, f"logs.{service}")


# ---------------------------------------------------------------------------
# 동기 발행 함수 (실제 Kafka I/O)
# ---------------------------------------------------------------------------

def publish_sync(service: str, value: str, key: str | None = None) -> None:
    """
    실제 Kafka로 보내는 동기 함수.
    asyncio 환경에서는 직접 호출하지 말고 publish() 를 await 할 것.
    """
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

    producer.produce(
        topic=topic,
        key=key.encode("utf-8") if key else None,
        value=value.encode("utf-8"),
        callback=_delivery_report,
    )
    producer.poll(0)


# ---------------------------------------------------------------------------
# 비동기 래퍼 (async/await 로 사용)
# ---------------------------------------------------------------------------

async def publish(service: str, value: str, key: str | None = None) -> None:
    """
    asyncio 환경에서 사용하는 비동기 발행 함수.

    내부에서는 publish_sync 를 thread pool 에서 실행하므로
    FastAPI 이벤트 루프를 막지 않는다.
    """
    loop = asyncio.get_running_loop()
    await loop.run_in_executor(None, publish_sync, service, value, key)
