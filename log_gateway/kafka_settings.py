# -----------------------------------------------------------------------------
# 파일명 : log_gateway/kafka_settings.py
# 목적   : 카프카/파이프라인 파라미터의 단일 진실(SSOT)
# -----------------------------------------------------------------------------
from __future__ import annotations
import os
from dataclasses import dataclass, field
from typing import Any, Dict, Optional


# ===== Kafka 설정 =====
@dataclass
class ProducerSettings:
    client_id: str = os.getenv("KAFKA_CLIENT_ID", "log-monitoring-simulator")
    brokers: str = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
    topics: Dict[str, str] = field(default_factory=lambda: {
        "auth":    "logs.auth",
        "order":   "logs.order",
        "payment": "logs.payment",
        "notify":  "logs.notify",
        "error":   "logs.error",
        # "dlq":  "dlq.logs",
    })
    security: Optional[Dict[str, Any]] = None  # 필요 시 SASL/SSL

KAFKA = ProducerSettings()

# confluent-kafka 기본 옵션
PRODUCER_BASE_CONFIG: Dict[str, Any] = {
    "bootstrap.servers": KAFKA.brokers,  # 접속할 브로커 목록
    "client.id": KAFKA.client_id,        # 모니터링용 프로듀서 ID
    "enable.idempotence": True,          # 멱등성 모드(중복 방지)
    "acks": "all",                       # 브로커 ack 정책(0/1/all)
    "compression.type": os.getenv("KAFKA_COMPRESSION", "gzip"),  # 압축 알고리즘(Kafka → zstd/gzip 등)
    "linger.ms": int(os.getenv("KAFKA_LINGER_MS", "5")),          # 프로듀서가 배치를 모을 시간(밀리초)
    "batch.num.messages": int(os.getenv("KAFKA_BATCH_NUM_MSG", "10000")),  # 배치당 최대 메시지 수
    # 필요 시 추가: queue.buffering.max.messages, queue.buffering.max.kbytes 등
}
if KAFKA.security:
    PRODUCER_BASE_CONFIG.update(KAFKA.security)
