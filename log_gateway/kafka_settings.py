# -----------------------------------------------------------------------------
# 파일명 : log_gateway/kafka_settings.py
# 목적   : 카프카/파이프라인 파라미터의 단일 진실(SSOT)
# -----------------------------------------------------------------------------
from __future__ import annotations
import os
from dataclasses import dataclass, field
from typing import Any, Dict, Optional


# ===== 파이프라인(생성/전송) 파라미터 =====
BATCH_MIN : int = 50
BATCH_MAX : int = 200
QUEUE_SIZE : int = 10000
PUBLISHER_WORKERS : int = 4


# # 퍼블리셔 튜닝(미니배치 드레인/폴링/백오프)
# WORKER_DRAIN_COUNT: int = int(os.getenv("LG_WORKER_DRAIN_COUNT", "5000"))
# WORKER_DRAIN_MS: int = int(os.getenv("LG_WORKER_DRAIN_MS", "5"))
# POLL_EVERY: int = int(os.getenv("LG_POLL_EVERY", "1000"))
# BUFFER_BACKOFF_MS: int = int(os.getenv("LG_BUFFER_BACKOFF_MS", "5"))

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
# zstd 미포함 환경을 고려해 기본은 gzip으로, 필요 시 KAFKA_COMPRESSION=zstd 등으로 오버라이드
PRODUCER_BASE_CONFIG: Dict[str, Any] = {
    "bootstrap.servers": KAFKA.brokers,
    "client.id": KAFKA.client_id,
    "enable.idempotence": True,
    "compression.type": os.getenv("KAFKA_COMPRESSION", "gzip"),
    "acks": "all",
    "linger.ms": int(os.getenv("KAFKA_LINGER_MS", "5")),
    "batch.num.messages": int(os.getenv("KAFKA_BATCH_NUM_MSG", "10000")),
    # 필요 시 추가: queue.buffering.max.messages, queue.buffering.max.kbytes 등
}
if KAFKA.security:
    PRODUCER_BASE_CONFIG.update(KAFKA.security)
