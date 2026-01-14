#!/usr/bin/env python3
"""
경량 Docker 모니터링 스크립트.
- kafka / spark / clickhouse 컨테이너 이벤트 및 로그를 감시
- OOM/StreamingQueryException 등 특정 키워드 또는 health 상태 변경 시 Slack/webhook 알림
- Prometheus 없이 빠르게 붙일 수 있는 최소 방어선
"""

from __future__ import annotations

import asyncio
import json
import os
import re
import shlex
import sys
from typing import Dict, Iterable, List
from urllib import request
from pathlib import Path


TARGET_CONTAINERS: List[str] = ["kafka", "spark", "clickhouse", "grafana"]
LOG_PATTERNS: Dict[str, List[re.Pattern[str]]] = {
    "kafka": [
        re.compile(r"OutOfMemoryError", re.IGNORECASE),
        re.compile(r"Fatal error", re.IGNORECASE),
    ],
    "spark": [
        re.compile(r"OutOfMemoryError"),
        re.compile(r"StreamingQueryException"),
        re.compile(r"Job aborted"),
    ],
    "clickhouse": [
        re.compile(r"Code:\s*241"),
        re.compile(r"Memory limit exceeded", re.IGNORECASE),
        re.compile(r"DB::Exception"),
    ],
    "grafana": [
        re.compile(r"level=error", re.IGNORECASE),
        re.compile(r"panic:", re.IGNORECASE),
    ],
}

ENV_PATH = Path(__file__).with_suffix(".env")
if not os.getenv("ALERT_WEBHOOK_URL") and ENV_PATH.exists():
    for line in ENV_PATH.read_text().splitlines():
        line = line.strip()
        if line.startswith("ALERT_WEBHOOK_URL="):
            value = line.split("=", 1)[1].strip().strip('"').strip("'")
            os.environ["ALERT_WEBHOOK_URL"] = value
            break
            
ALERT_WEBHOOK_URL = os.getenv("ALERT_WEBHOOK_URL")  # Slack 등 Webhook URL
HEALTH_INTERVAL_SEC = 30


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


async def stream_lines(cmd: Iterable[str]) -> tuple[asyncio.subprocess.Process, asyncio.StreamReader]:
    """지정 명령을 실행하고 stdout을 라인 단위로 yield."""
    proc = await asyncio.create_subprocess_exec(
        *cmd,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.STDOUT,
    )
    assert proc.stdout is not None
    return proc, proc.stdout


async def watch_events() -> None:
    """docker events 출력에서 대상 컨테이너 상태 변화를 감지."""
    cmd = [
        "docker",
        "events",
        "--format",
        "{{json .}}",
        "--filter",
        "type=container",
    ]
    proc, stdout = await stream_lines(cmd)
    while True:
        line = await stdout.readline()
        if not line:
            await asyncio.sleep(1)
            continue
        payload = line.decode(errors="ignore").strip()
        if not payload:
            continue
        try:
            event = json.loads(payload)
        except json.JSONDecodeError:
            continue
        actor = event.get("Actor", {})
        attributes = actor.get("Attributes", {})
        name = attributes.get("name") or actor.get("ID")
        if name not in TARGET_CONTAINERS:
            continue
        status = event.get("status", "")
        if not status or status.startswith("exec_"):
            continue  # docker exec 관련 이벤트는 무시
        if "die" in status or "health_status: unhealthy" in status:
            await send_alert(f"{name} status changed: {status}")


async def watch_logs(container: str, patterns: List[re.Pattern[str]]) -> None:
    """docker logs -f 로 특정 키워드 감지."""
    cmd = [
        "docker",
        "logs",
        "-f",
        "--since",
        "1s",
        container,
    ]
    proc, stdout = await stream_lines(cmd)
    while True:
        line = await stdout.readline()
        if not line:
            await asyncio.sleep(1)
            continue
        text = line.decode(errors="ignore").rstrip()
        if any(p.search(text) for p in patterns):
            await send_alert(f"{container} log matched: {text}")


async def check_health_loop() -> None:
    """주기적으로 docker inspect 를 호출해 health 상태 점검."""
    while True:
        for container in TARGET_CONTAINERS:
            cmd = [
                "docker",
                "inspect",
                "-f",
                "{{.State.Health.Status}}",
                container,
            ]
            proc = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            stdout, _ = await proc.communicate()
            status = stdout.decode().strip()
            if status and status != "healthy":
                await send_alert(f"{container} health={status}")
        await asyncio.sleep(HEALTH_INTERVAL_SEC)


async def spark_rest_probe() -> None:
    """Spark UI REST API를 주기적으로 호출해 응답성을 확인."""
    import urllib.error
    import http.client

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
    import urllib.error
    import http.client

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


async def main() -> None:
    """main 처리를 수행한다."""
    tasks = [
        asyncio.create_task(watch_events()),
        asyncio.create_task(check_health_loop()),
        asyncio.create_task(spark_rest_probe()),
        asyncio.create_task(grafana_health_probe()),
    ]
    for container, pats in LOG_PATTERNS.items():
        tasks.append(asyncio.create_task(watch_logs(container, pats)))

    await asyncio.gather(*tasks)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("watchdog stopped.")
