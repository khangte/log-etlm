# -----------------------------------------------------------------------------
# 파일명 : simulator/core/kafka.py
# 목적   : Kafka 관련 설정/유틸만 별도 모듈로 분리
# 사용   : runner/producer 등에서 import하여 토픽/프로듀서 설정 접근
# 주의   : .env 사용 금지. 이 파일이 Kafka 설정의 단일 진실 소스(SSOT)
# -----------------------------------------------------------------------------

from __future__ import annotations
from typing import Dict, Any

# ===== 브로커/클라이언트/토픽 =====
BROKERS: str = "localhost:9092"
CLIENT_ID: str = "log-forge-sim"   # 프로젝트명에 맞춤

# 서비스별 토픽 맵 + error/DLQ (DLQ는 선택)
TOPICS: Dict[str, str] = {
    "auth":    "logs.auth",
    "order":   "logs.order",
    "payment": "logs.payment",
    "notify":  "logs.notify",
    "error":   "logs.error",   # 에러 복제 발행용
    "dlq":     "dlq.logs",     # 파싱 실패 등 사후 처리용(선택)
}

# (선택) 보안 설정. 기본은 None(로컬)
SECURITY: Dict[str, Any] | None = None
# 예시)
# SECURITY = {
#     "security.protocol": "SASL_SSL",
#     "sasl.mechanism": "PLAIN",
#     "sasl.username": "user",
#     "sasl.password": "pass",
# }


def get_topics() -> Dict[str, str]:
    """
    Kafka 토픽 맵을 반환한다.

    Returns:
        Dict[str, str]: {"auth":"logs.auth", ... "error":"logs.error", "dlq":"dlq.logs"}
    """
    return TOPICS.copy()


def get_producer_config() -> Dict[str, Any]:
    """
    Kafka Producer 생성에 필요한 기본 설정을 반환한다.
    - 멱등성/압축/배치/acks 등 안정성/성능 옵션 포함.

    Returns:
        Dict[str, Any]: confluent-kafka/python-kafka 등에서 사용할 설정 맵
    """
    base = {
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
        base.update(SECURITY)
    return base
