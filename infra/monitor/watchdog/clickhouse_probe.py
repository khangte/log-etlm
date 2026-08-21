from __future__ import annotations

import asyncio
import json
from urllib import request, error
from urllib.parse import urlencode

from .alert import send_alert, should_alert, breach_ready
from .config import (
    CH_MONITOR_ENABLED,
    CH_HTTP_URL,
    CH_DB,
    CH_USER,
    CH_PASSWORD,
    CH_TIMEOUT_SEC,
    CH_QUERY_INTERVAL_SEC,
    P95_PRODUCER_TO_KAFKA_MS_MAX,
    P95_KAFKA_TO_SPARK_INGEST_MS_MAX,
    P95_SPARK_PROCESSING_MS_MAX,
    P95_SPARK_TO_STORED_MS_MAX,
    P95_E2E_MS_MAX,
    FRESHNESS_MS_MAX,
    EPS_MIN,
    ERROR_RATE_PCT_MAX,
)

_SQL_STAGE = """
SELECT
  eps.bucket AS eps_bucket,
  eps.eps AS eps,
  eps.error_rate_pct AS error_rate_pct,
  lat_s.bucket AS latency_bucket,
  lat_s.producer_to_kafka_p95_ms AS producer_to_kafka_p95_ms,
  lat_s.kafka_to_spark_ingest_p95_ms AS kafka_to_spark_ingest_p95_ms,
  lat_s.spark_processing_p95_ms AS spark_processing_p95_ms,
  lat_s.spark_to_stored_p95_ms AS spark_to_stored_p95_ms,
  lat_s.e2e_p95_ms AS e2e_p95_ms,
  fresh.freshness_ms AS freshness_ms
FROM
  (SELECT 1 AS k) AS base
LEFT JOIN
  (
    SELECT
      1 AS k,
      bucket,
      if(total = 0, 0, errors / total) * 100 AS error_rate_pct,
      total / 60 AS eps
    FROM (
      SELECT
        bucket,
        countMerge(total_state) AS total,
        countMerge(errors_state) AS errors
      FROM analytics.event_log_agg_1m
      WHERE bucket >= now() - INTERVAL 5 MINUTE
      GROUP BY bucket
      ORDER BY bucket DESC
      LIMIT 1
    )
  ) AS eps
ON base.k = eps.k
LEFT JOIN
  (
    SELECT
      1 AS k,
      bucket,
      quantileTDigestMerge(0.95)(producer_to_kafka_state) AS producer_to_kafka_p95_ms,
      quantileTDigestMerge(0.95)(kafka_to_spark_ingest_state) AS kafka_to_spark_ingest_p95_ms,
      quantileTDigestMerge(0.95)(spark_processing_state) AS spark_processing_p95_ms,
      quantileTDigestMerge(0.95)(spark_to_stored_state) AS spark_to_stored_p95_ms,
      quantileTDigestMerge(0.95)(e2e_state) AS e2e_p95_ms
    FROM analytics.event_log_latency_service_1m
    WHERE bucket >= now() - INTERVAL 5 MINUTE
    GROUP BY bucket
    ORDER BY bucket DESC
    LIMIT 1
  ) AS lat_s
ON base.k = lat_s.k
LEFT JOIN
  (
    SELECT
      1 AS k,
      dateDiff('millisecond', max(kafka_received_at), now()) AS freshness_ms
    FROM analytics.event_log
    WHERE kafka_received_at >= now() - INTERVAL 10 MINUTE
  ) AS fresh
ON base.k = fresh.k
""".strip()


def _ch_query_sync(sql: str) -> dict[str, object] | None:
    params = {"database": CH_DB, "default_format": "JSON"}
    url = f"{CH_HTTP_URL.rstrip('/')}/?{urlencode(params)}"
    headers = {"Content-Type": "text/plain; charset=utf-8"}
    if CH_USER:
        headers["X-ClickHouse-User"] = CH_USER
    if CH_PASSWORD:
        headers["X-ClickHouse-Key"] = CH_PASSWORD
    req = request.Request(url, data=sql.encode("utf-8"), headers=headers)
    try:
        with request.urlopen(req, timeout=CH_TIMEOUT_SEC) as resp:
            payload = json.loads(resp.read())
    except error.HTTPError as exc:
        body = ""
        try:
            raw = exc.read()
            if raw:
                body = raw.decode("utf-8", errors="ignore").strip()
        except Exception:
            body = ""
        detail = f"{exc}" + (f" body={body}" if body else "")
        raise RuntimeError(detail) from None
    rows = payload.get("data", [])
    return rows[0] if rows else None


async def _ch_query(sql: str) -> dict[str, object] | None:
    return await asyncio.to_thread(_ch_query_sync, sql)


def _as_float(value: object) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


async def clickhouse_stage_probe() -> None:
    if not CH_MONITOR_ENABLED:
        return

    while True:
        now = asyncio.get_running_loop().time()
        try:
            row = await _ch_query(_SQL_STAGE)
        except Exception as exc:
            if should_alert("clickhouse_query_error", now):
                await send_alert(f"clickhouse query failed: {exc}")
            await asyncio.sleep(CH_QUERY_INTERVAL_SEC)
            continue

        if row:
            await _check_metrics(row, now)

        await asyncio.sleep(CH_QUERY_INTERVAL_SEC)


async def _check_threshold(
    key: str,
    now: float,
    value: float | None,
    threshold: float,
    template: str,
    *,
    below: bool = False,
    **fields: object,
) -> None:
    """임계값 위반을 판정하고 grace/cooldown을 거쳐 알림을 보낸다."""
    breached = value is not None and (value < threshold if below else value > threshold)
    if breached and breach_ready(key, now, True) and should_alert(key, now):
        await send_alert(template.format(value=value, threshold=threshold, **fields))
    if not breached:
        breach_ready(key, now, False)


_LATENCY_CHECKS = (
    ("producer_to_kafka_p95",     "producer_to_kafka_p95_ms",     P95_PRODUCER_TO_KAFKA_MS_MAX,     "producer_to_kafka"),
    ("kafka_to_spark_ingest_p95", "kafka_to_spark_ingest_p95_ms", P95_KAFKA_TO_SPARK_INGEST_MS_MAX, "kafka_to_spark_ingest"),
    ("spark_processing_p95",      "spark_processing_p95_ms",      P95_SPARK_PROCESSING_MS_MAX,      "spark_processing"),
    ("spark_to_stored_p95",       "spark_to_stored_p95_ms",       P95_SPARK_TO_STORED_MS_MAX,       "spark_to_stored"),
    ("e2e_p95",                   "e2e_p95_ms",                   P95_E2E_MS_MAX,                   "e2e"),
)


async def _check_metrics(row: dict[str, object], now: float) -> None:
    eps_bucket = row.get("eps_bucket")
    await _check_threshold(
        "eps_low", now, _as_float(row.get("eps")), EPS_MIN,
        "stage=ingest eps={value:.2f} threshold={threshold} bucket={bucket}",
        below=True, bucket=eps_bucket,
    )
    await _check_threshold(
        "error_rate", now, _as_float(row.get("error_rate_pct")), ERROR_RATE_PCT_MAX,
        "stage=ingest error_rate_pct={value:.3f} threshold={threshold} bucket={bucket}",
        bucket=eps_bucket,
    )
    latency_bucket = row.get("latency_bucket")
    for key, column, threshold, stage in _LATENCY_CHECKS:
        await _check_threshold(
            key, now, _as_float(row.get(column)), threshold,
            "stage={stage} p95_ms={value:.0f} threshold_ms={threshold} bucket={bucket}",
            stage=stage, bucket=latency_bucket,
        )
    await _check_threshold(
        "freshness", now, _as_float(row.get("freshness_ms")), FRESHNESS_MS_MAX,
        "stage=freshness ms={value:.0f} threshold_ms={threshold}",
    )
