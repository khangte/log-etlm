#!/usr/bin/env python3
"""
경량 Docker 모니터링 스크립트.
- kafka / spark / clickhouse 컨테이너 이벤트 및 로그를 감시
- ClickHouse 기반 단계별 지표를 점검해 이상 시 알림
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
from urllib.parse import urlencode


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
if ENV_PATH.exists():
    for line in ENV_PATH.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        if key and key not in os.environ:
            os.environ[key] = value
            
ALERT_WEBHOOK_URL = os.getenv("ALERT_WEBHOOK_URL")  # Slack 등 Webhook URL
HEALTH_INTERVAL_SEC = 30
ALERT_COOLDOWN_SEC = int(os.getenv("ALERT_COOLDOWN_SEC", "300"))

CH_MONITOR_ENABLED = os.getenv("CH_MONITOR_ENABLED", "true").lower() == "true"
CH_HTTP_URL = os.getenv("CH_HTTP_URL", "http://localhost:8123")
CH_DB = os.getenv("CH_DB", "analytics")
CH_USER = os.getenv("CH_USER", os.getenv("CLICKHOUSE_USER", ""))
CH_PASSWORD = os.getenv("CH_PASSWORD", os.getenv("CLICKHOUSE_PASSWORD", ""))
CH_TIMEOUT_SEC = int(os.getenv("CH_TIMEOUT_SEC", "5"))
CH_QUERY_INTERVAL_SEC = int(os.getenv("CH_QUERY_INTERVAL_SEC", "300"))

P95_QUEUE_MS_MAX = int(os.getenv("P95_QUEUE_MS_MAX", "60000"))
P95_PUBLISH_MS_MAX = int(os.getenv("P95_PUBLISH_MS_MAX", "60000"))
P95_SINK_MS_MAX = int(os.getenv("P95_SINK_MS_MAX", "60000"))
P95_E2E_MS_MAX = int(os.getenv("P95_E2E_MS_MAX", "60000"))
FRESHNESS_MS_MAX = int(os.getenv("FRESHNESS_MS_MAX", "120000"))
EPS_MIN = float(os.getenv("EPS_MIN", "1"))
ERROR_RATE_PCT_MAX = float(os.getenv("ERROR_RATE_PCT_MAX", "1"))
DLQ_RATE_PCT_MAX = float(os.getenv("DLQ_RATE_PCT_MAX", "1"))

_last_alert_at: Dict[str, float] = {}


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


def _should_alert(key: str, now: float) -> bool:
    last = _last_alert_at.get(key)
    if last is None or now - last >= ALERT_COOLDOWN_SEC:
        _last_alert_at[key] = now
        return True
    return False


def _build_ch_url() -> str:
    base = CH_HTTP_URL.rstrip("/") + "/"
    params = {"database": CH_DB, "default_format": "JSON"}
    if CH_USER:
        params["user"] = CH_USER
    if CH_PASSWORD:
        params["password"] = CH_PASSWORD
    return f"{base}?{urlencode(params)}"


def _ch_query_sync(sql: str) -> Dict[str, object] | None:
    req = request.Request(
        _build_ch_url(),
        data=sql.encode("utf-8"),
        headers={"Content-Type": "text/plain; charset=utf-8"},
    )
    with request.urlopen(req, timeout=CH_TIMEOUT_SEC) as resp:
        payload = json.loads(resp.read())
    rows = payload.get("data", [])
    if not rows:
        return None
    return rows[0]


async def _ch_query(sql: str) -> Dict[str, object] | None:
    return await asyncio.to_thread(_ch_query_sync, sql)


def _as_float(value: object) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


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


async def clickhouse_stage_probe() -> None:
    if not CH_MONITOR_ENABLED:
        return

    sql_stage = """
SELECT
  eps.bucket AS eps_bucket,
  eps.eps AS eps,
  eps.error_rate_pct AS error_rate_pct,
  lat_s.bucket AS latency_service_bucket,
  lat_s.queue_p95_ms AS queue_p95_ms,
  lat_s.publish_p95_ms AS publish_p95_ms,
  lat.bucket AS latency_bucket,
  lat.sink_p95_ms AS sink_p95_ms,
  lat.e2e_p95_ms AS e2e_p95_ms,
  fresh.freshness_ms AS freshness_ms,
  dlq.dlq_rate_pct AS dlq_rate_pct
FROM
  (SELECT 1 AS k) AS base
LEFT JOIN
  (
    SELECT
      bucket,
      if(total = 0, 0, errors / total) * 100 AS error_rate_pct,
      total / 60 AS eps
    FROM (
      SELECT
        bucket,
        uniqCombined64Merge(total_state) AS total,
        uniqCombined64Merge(errors_state) AS errors
      FROM analytics.fact_event_agg_1m
      WHERE bucket >= now() - INTERVAL 5 MINUTE
      GROUP BY bucket
      ORDER BY bucket DESC
      LIMIT 1
    )
  ) AS eps
ON base.k = 1
LEFT JOIN
  (
    SELECT
      bucket,
      quantileTDigestMerge(0.95)(queue_state) AS queue_p95_ms,
      quantileTDigestMerge(0.95)(publish_state) AS publish_p95_ms
    FROM analytics.fact_event_latency_service_1m
    WHERE bucket >= now() - INTERVAL 5 MINUTE
    GROUP BY bucket
    ORDER BY bucket DESC
    LIMIT 1
  ) AS lat_s
ON base.k = 1
LEFT JOIN
  (
    SELECT
      bucket,
      quantileTDigestMerge(0.95)(sink_state) AS sink_p95_ms,
      quantileTDigestMerge(0.95)(e2e_state) AS e2e_p95_ms
    FROM analytics.fact_event_latency_1m
    WHERE bucket >= now() - INTERVAL 5 MINUTE
    GROUP BY bucket
    ORDER BY bucket DESC
    LIMIT 1
  ) AS lat
ON base.k = 1
LEFT JOIN
  (
    SELECT
      dateDiff('millisecond', max(ingest_ts), now()) AS freshness_ms
    FROM analytics.fact_event
    WHERE ingest_ts >= now() - INTERVAL 10 MINUTE
  ) AS fresh
ON base.k = 1
LEFT JOIN
  (
    SELECT
      if(t.total = 0, 0, d.dlq / t.total) * 100 AS dlq_rate_pct
    FROM
    (
      SELECT sum(total) AS dlq
      FROM analytics.fact_event_dlq_agg_1m
      WHERE bucket >= now() - INTERVAL 5 MINUTE
    ) AS d
    CROSS JOIN
    (
      SELECT uniqCombined64Merge(total_state) AS total
      FROM analytics.fact_event_agg_1m
      WHERE bucket >= now() - INTERVAL 5 MINUTE
    ) AS t
  ) AS dlq
ON base.k = 1
""".strip()

    while True:
        now = asyncio.get_running_loop().time()
        try:
            stage_row = await _ch_query(sql_stage)
        except Exception as exc:
            if _should_alert("clickhouse_query_error", now):
                await send_alert(f"clickhouse query failed: {exc}")
            await asyncio.sleep(CH_QUERY_INTERVAL_SEC)
            continue

        if stage_row:
            eps = _as_float(stage_row.get("eps"))
            err = _as_float(stage_row.get("error_rate_pct"))
            eps_bucket = stage_row.get("eps_bucket")
            if eps is not None and eps < EPS_MIN and _should_alert("eps_low", now):
                await send_alert(
                    f"stage=ingest eps={eps:.2f} threshold={EPS_MIN} bucket={eps_bucket}"
                )
            if err is not None and err > ERROR_RATE_PCT_MAX and _should_alert("error_rate", now):
                await send_alert(
                    "stage=ingest error_rate_pct="
                    f"{err:.3f} threshold={ERROR_RATE_PCT_MAX} bucket={eps_bucket}"
                )

        if stage_row:
            latency_service_bucket = stage_row.get("latency_service_bucket")
            queue_p95 = _as_float(stage_row.get("queue_p95_ms"))
            publish_p95 = _as_float(stage_row.get("publish_p95_ms"))
            if queue_p95 is not None and queue_p95 > P95_QUEUE_MS_MAX and _should_alert(
                "queue_p95", now
            ):
                await send_alert(
                    f"stage=queue p95_ms={queue_p95:.0f} "
                    f"threshold_ms={P95_QUEUE_MS_MAX} bucket={latency_service_bucket}"
                )
            if publish_p95 is not None and publish_p95 > P95_PUBLISH_MS_MAX and _should_alert(
                "publish_p95", now
            ):
                await send_alert(
                    f"stage=publish p95_ms={publish_p95:.0f} "
                    f"threshold_ms={P95_PUBLISH_MS_MAX} bucket={latency_service_bucket}"
                )

        if stage_row:
            latency_bucket = stage_row.get("latency_bucket")
            sink_p95 = _as_float(stage_row.get("sink_p95_ms"))
            e2e_p95 = _as_float(stage_row.get("e2e_p95_ms"))
            if sink_p95 is not None and sink_p95 > P95_SINK_MS_MAX and _should_alert(
                "sink_p95", now
            ):
                await send_alert(
                    f"stage=sink p95_ms={sink_p95:.0f} "
                    f"threshold_ms={P95_SINK_MS_MAX} bucket={latency_bucket}"
                )
            if e2e_p95 is not None and e2e_p95 > P95_E2E_MS_MAX and _should_alert(
                "e2e_p95", now
            ):
                await send_alert(
                    f"stage=e2e p95_ms={e2e_p95:.0f} "
                    f"threshold_ms={P95_E2E_MS_MAX} bucket={latency_bucket}"
                )

        if stage_row:
            freshness = _as_float(stage_row.get("freshness_ms"))
            if freshness is not None and freshness > FRESHNESS_MS_MAX and _should_alert(
                "freshness", now
            ):
                await send_alert(
                    f"stage=freshness ms={freshness:.0f} threshold_ms={FRESHNESS_MS_MAX}"
                )

        if stage_row:
            dlq_rate = _as_float(stage_row.get("dlq_rate_pct"))
            if dlq_rate is not None and dlq_rate > DLQ_RATE_PCT_MAX and _should_alert(
                "dlq_rate", now
            ):
                await send_alert(
                    f"stage=dlq rate_pct={dlq_rate:.3f} threshold={DLQ_RATE_PCT_MAX}"
                )

        await asyncio.sleep(CH_QUERY_INTERVAL_SEC)


async def main() -> None:
    """main 처리를 수행한다."""
    tasks = [
        asyncio.create_task(watch_events()),
        asyncio.create_task(check_health_loop()),
        asyncio.create_task(spark_rest_probe()),
        asyncio.create_task(grafana_health_probe()),
        asyncio.create_task(clickhouse_stage_probe()),
    ]
    for container, pats in LOG_PATTERNS.items():
        tasks.append(asyncio.create_task(watch_logs(container, pats)))

    await asyncio.gather(*tasks)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("watchdog stopped.")
