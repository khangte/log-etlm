# -----------------------------------------------------------------------------
# file: log_simulator/generator/batch_builder.py
# purpose: build batch items for publish queue from simulator events
# -----------------------------------------------------------------------------

from __future__ import annotations

from typing import Any, Iterable, List

from ..models.messages import BatchMessage


def apply_created_ms(events: Iterable[dict], created_ms: int) -> None:
    for ev in events:
        ev.setdefault("created_ms", created_ms)


def build_batch_items(
    service: str,
    simulator: Any,
    events: List[dict],
    enqueued_ms: int,
) -> List[BatchMessage]:
    batch_items: List[BatchMessage] = []
    for ev in events:
        ev["enqueued_ms"] = enqueued_ms
        payload = simulator.render_bytes(ev)
        request_id = ev.get("request_id")
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
    result = ev.get("result")
    if result is not None:
        return (result == "fail")
    status_code = ev.get("status_code")
    if isinstance(status_code, int):
        return status_code >= 500
    return False
