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
    if not service:
        return "logs.unknown"
    return TOPICS.get(service, f"logs.{service}")
