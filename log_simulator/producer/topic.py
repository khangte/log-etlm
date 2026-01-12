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
    if not service:
        return TOPICS["unknown"]
    return TOPICS.get(service, f"logs.{service}")
