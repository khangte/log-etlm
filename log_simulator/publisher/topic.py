# -----------------------------------------------------------------------------
# file: log_simulator/publisher/topic.py
# purpose: service-to-topic mapping
# -----------------------------------------------------------------------------

from __future__ import annotations

from typing import Dict


TOPICS: Dict[str, str] = {
    "auth": "logs.event",
    "order": "logs.event",
    "payment": "logs.event",
    "dlq": "logs.dlq",
}


def get_topic(service: str) -> str:
    return TOPICS.get(service, "logs.error")
