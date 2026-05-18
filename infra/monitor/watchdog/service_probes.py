from __future__ import annotations

import asyncio
import http.client
import urllib.error
from urllib import request

from .alert import send_alert
from .config import HEALTH_INTERVAL_SEC


async def spark_rest_probe() -> None:
    """Spark UI REST API를 주기적으로 호출해 응답성을 확인."""
    url = "http://localhost:4040/api/v1/applications"
    while True:
        try:
            with request.urlopen(url, timeout=5) as resp:
                if resp.status != 200:
                    await send_alert(f"spark REST returned HTTP {resp.status}")
        except (
            urllib.error.URLError,
            TimeoutError,
            ConnectionResetError,
            http.client.RemoteDisconnected,
            OSError,
        ) as exc:
            await send_alert(f"spark REST unreachable: {exc}")
        except Exception as exc:
            await send_alert(f"spark REST probe error: {exc}")
        await asyncio.sleep(HEALTH_INTERVAL_SEC)


async def grafana_health_probe() -> None:
    """Grafana HTTP API를 간단히 확인."""
    url = "http://localhost:3000/api/health"
    while True:
        try:
            with request.urlopen(url, timeout=5) as resp:
                if resp.status != 200:
                    await send_alert(f"grafana health returned HTTP {resp.status}")
        except (
            urllib.error.URLError,
            TimeoutError,
            ConnectionResetError,
            http.client.RemoteDisconnected,
            OSError,
        ) as exc:
            await send_alert(f"grafana health unreachable: {exc}")
        except Exception as exc:
            await send_alert(f"grafana health probe error: {exc}")
        await asyncio.sleep(HEALTH_INTERVAL_SEC)
