from __future__ import annotations

from typing import Dict


TOPICS: Dict[str, str] = {
    "auth": "logs.auth",
    "order": "logs.order",
    "payment": "logs.payment",
    "dlq": "logs.dlq",
    "unknown": "logs.unknown",
}

def get_topic(service: str) -> str:
    """서비스 이름에 대응하는 Kafka 토픽명을 반환한다."""
    if not service:
        return TOPICS["unknown"]
    return TOPICS.get(service, f"logs.{service}")
