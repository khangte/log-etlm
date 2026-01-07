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
    """서비스에 대응하는 토픽을 반환한다."""
    return TOPICS.get(service, "logs.error")
