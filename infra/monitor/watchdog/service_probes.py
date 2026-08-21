from __future__ import annotations

import asyncio
from urllib import request

from .alert import send_alert
from .config import HEALTH_INTERVAL_SEC

HTTP_PROBES: tuple[tuple[str, str], ...] = (
    ("spark REST", "http://localhost:4040/api/v1/applications"),
    ("grafana health", "http://localhost:3000/api/health"),
)


async def http_probe(name: str, url: str) -> None:
    """HTTP 엔드포인트를 주기적으로 호출해 응답성을 확인한다."""
    while True:
        try:
            with request.urlopen(url, timeout=5) as resp:
                if resp.status != 200:
                    await send_alert(f"{name} returned HTTP {resp.status}")
        except OSError as exc:
            await send_alert(f"{name} unreachable: {exc}")
        except Exception as exc:
            await send_alert(f"{name} probe error: {exc}")
        await asyncio.sleep(HEALTH_INTERVAL_SEC)
