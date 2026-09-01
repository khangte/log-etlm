from __future__ import annotations

from typing import Dict


TOPICS: Dict[str, str] = {
    "auth": "logs.auth",
    "order": "logs.order",
    "payment": "logs.payment",
    "error": "logs.error",
    "dlq": "logs.dlq",
}

def get_topic(service: str) -> str:
    """서비스명을 토픽명으로 매핑한다. 미등록 서비스는 KeyError."""
    return TOPICS[service]
