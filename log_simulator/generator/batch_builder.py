# -----------------------------------------------------------------------------
# 파일명 : log_simulator/generator/batch_builder.py
# 목적   : 시뮬레이터 이벤트를 퍼블리시 큐 배치 항목으로 변환
# -----------------------------------------------------------------------------

from __future__ import annotations

from typing import Iterable, List

from ..models.messages import BatchMessage


def apply_created_ms(events: Iterable[dict], created_ms: int) -> None:
    """이벤트에 created_ms 타임스탬프를 채운다."""
    for ev in events:
        ev.setdefault("created_ms", created_ms)


def build_batch_items(
    service: str,
    events: List[dict],
    enqueued_ms: int,
) -> List[BatchMessage]:
    """이벤트 목록을 BatchMessage 목록으로 변환한다."""
    batch_items: List[BatchMessage] = []
    for ev in events:
        ev["enqueued_ms"] = enqueued_ms
        payload = ev
        request_id = ev.get("rid") or ev.get("request_id")
        key = str(request_id).encode("utf-8") if request_id else None
        batch_items.append(
            BatchMessage(
                service=service,
                value=payload,
                key=key,
                replicate_error=_is_err_event(ev),
                enqueued_ms=enqueued_ms,
            )
        )
    return batch_items


def _is_err_event(ev: dict) -> bool:
    """이벤트 실패 여부를 판단한다."""
    result = ev.get("res") or ev.get("result")
    if result is not None:
        return (result == "fail")
    return False
