from __future__ import annotations

import json
import sys
from urllib import request

from .config import ALERT_WEBHOOK_URL, ALERT_COOLDOWN_SEC, ALERT_BREACH_GRACE_SEC

_last_alert_at: dict[str, float] = {}
_breach_since: dict[str, float] = {}


async def send_alert(message: str) -> None:
    """Webhook 또는 표준출력으로 경보 전송."""
    text = f"[watchdog] {message}"
    print(text, flush=True)

    if not ALERT_WEBHOOK_URL:
        return

    data = json.dumps({"text": text}).encode("utf-8")
    req = request.Request(
        ALERT_WEBHOOK_URL,
        data=data,
        headers={"Content-Type": "application/json"},
    )
    try:
        with request.urlopen(req, timeout=5) as resp:
            if resp.status >= 400:
                print(f"[watchdog] webhook failed: HTTP {resp.status}", file=sys.stderr)
    except Exception as exc:
        print(f"[watchdog] webhook error: {exc}", file=sys.stderr)


def should_alert(key: str, now: float) -> bool:
    last = _last_alert_at.get(key)
    if last is None or now - last >= ALERT_COOLDOWN_SEC:
        _last_alert_at[key] = now
        return True
    return False


def breach_ready(key: str, now: float, breached: bool) -> bool:
    """임계값 초과가 grace 기간 이상 유지되었는지 확인한다."""
    if not breached:
        _breach_since.pop(key, None)
        return False
    first = _breach_since.get(key)
    if first is None:
        _breach_since[key] = now
        return False
    return (now - first) >= ALERT_BREACH_GRACE_SEC
