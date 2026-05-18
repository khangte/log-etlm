from __future__ import annotations

import asyncio
import json
import re
from collections.abc import Iterable

from .alert import send_alert
from .config import TARGET_CONTAINERS, HEALTH_INTERVAL_SEC


async def stream_lines(
    cmd: Iterable[str],
) -> tuple[asyncio.subprocess.Process, asyncio.StreamReader]:
    """지정 명령을 실행하고 stdout을 라인 단위로 읽을 수 있는 스트림을 반환한다."""
    proc = await asyncio.create_subprocess_exec(
        *cmd,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.STDOUT,
    )
    if proc.stdout is None:
        raise RuntimeError(f"stdout pipe unavailable for command: {list(cmd)}")
    return proc, proc.stdout


async def watch_events() -> None:
    """docker events 출력에서 대상 컨테이너 상태 변화를 감지."""
    cmd = [
        "docker", "events",
        "--format", "{{json .}}",
        "--filter", "type=container",
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
            continue
        if "die" in status or "health_status: unhealthy" in status:
            await send_alert(f"{name} status changed: {status}")


async def watch_logs(container: str, patterns: list[re.Pattern[str]]) -> None:
    """docker logs -f 로 특정 키워드 감지."""
    cmd = ["docker", "logs", "-f", "--since", "1s", container]
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
                "docker", "inspect",
                "-f", "{{.State.Health.Status}}",
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
