# -----------------------------------------------------------------------------
# 파일명 : log_simulator/publisher/dlq.py
# 목적   : 전송 실패 배치를 DLQ 메시지로 변환/전송
# -----------------------------------------------------------------------------

from __future__ import annotations

import json
import logging
import time
from typing import Sequence

from ..models.messages import BatchMessage
from .producer import publish_batch_direct, publish_batch_direct_sync
from .topic import get_topic


_logger = logging.getLogger("log_simulator.dlq")
_logger.setLevel(logging.INFO)
if not _logger.handlers:
    handler = logging.StreamHandler()
    handler.setFormatter(
        logging.Formatter("%(asctime)s [%(levelname)s] %(name)s - %(message)s")
    )
    _logger.addHandler(handler)


def build_dlq_message(message: BatchMessage, error: Exception) -> BatchMessage:
    """실패 배치를 DLQ 메시지로 변환한다."""
    raw_json = ""
    event_id = None
    request_id = None
    payload = None

    if isinstance(message.value, dict):
        payload = message.value
        event_id = payload.get("event_id")
        request_id = payload.get("request_id")
        raw_json = json.dumps(payload, ensure_ascii=False)
    elif isinstance(message.value, bytes):
        raw_json = message.value.decode("utf-8", errors="replace")
        try:
            payload = json.loads(raw_json)
            event_id = payload.get("event_id")
            request_id = payload.get("request_id")
        except Exception:
            pass
    else:
        raw_json = str(message.value)
        try:
            payload = json.loads(raw_json)
            event_id = payload.get("event_id")
            request_id = payload.get("request_id")
        except Exception:
            pass

    dlq_payload = {
        "error_type": type(error).__name__,
        "error_message": str(error),
        "service": message.service,
        "event_id": event_id,
        "request_id": request_id,
        "source_topic": get_topic(message.service),
        "created_ms": int(time.time() * 1000),
        "raw_json": raw_json,
    }

    key = request_id or event_id
    return BatchMessage(
        service="dlq",
        value=json.dumps(dlq_payload, ensure_ascii=True).encode("utf-8"),
        key=key.encode("utf-8") if key else None,
        replicate_error=False,
    )


async def publish_dlq_batch(batch: Sequence[BatchMessage], error: Exception) -> None:
    """DLQ 배치를 비동기로 전송한다."""
    dlq_batch = [build_dlq_message(message, error) for message in batch]
    try:
        await publish_batch_direct(dlq_batch)
        _logger.warning(
            "[publisher] sent to dlq batch=%d error=%s",
            len(dlq_batch),
            type(error).__name__,
        )
    except Exception:
        _logger.exception("[publisher] dlq send failed batch=%d", len(dlq_batch))


def publish_dlq_batch_sync(batch: Sequence[BatchMessage], error: Exception) -> None:
    """DLQ 배치를 동기로 전송한다."""
    dlq_batch = [build_dlq_message(message, error) for message in batch]
    try:
        publish_batch_direct_sync(dlq_batch)
        _logger.warning(
            "[publisher] sent to dlq batch=%d error=%s",
            len(dlq_batch),
            type(error).__name__,
        )
    except Exception:
        _logger.exception("[publisher] dlq send failed batch=%d", len(dlq_batch))
