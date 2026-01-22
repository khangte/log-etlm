#!/usr/bin/env bash
set -euo pipefail

# 용도: ClickHouse 지연/eps 지표를 정리해서 publish/queue/e2e 병목 구간을 빠르게 파악하게 해주는 진단 스크립트입니다.

WINDOW_MIN="${WINDOW_MIN:-10}"
CLICKHOUSE_CONTAINER="${CLICKHOUSE_CONTAINER:-clickhouse}"
CLICKHOUSE_USER="${CLICKHOUSE_USER:-log_user}"
CLICKHOUSE_PASSWORD="${CLICKHOUSE_PASSWORD:-log_pwd}"
CLICKHOUSE_DB="${CLICKHOUSE_DB:-analytics}"

run_query() {
  local sql="$1"
  docker exec -i "${CLICKHOUSE_CONTAINER}" clickhouse-client \
    -u "${CLICKHOUSE_USER}" --password "${CLICKHOUSE_PASSWORD}" \
    --query "${sql}"
}

echo "== publish/queue/e2e p95 (last ${WINDOW_MIN}m) =="
run_query "
SELECT
  bucket,
  quantileTDigestMerge(0.95)(queue_state) AS queue_p95_ms,
  quantileTDigestMerge(0.95)(publish_state) AS publish_p95_ms,
  quantileTDigestMerge(0.95)(kafka_to_processed_state) AS kafka_to_processed_p95_ms,
  quantileTDigestMerge(0.95)(ingest_to_kafka_state) AS ingest_to_kafka_p95_ms,
  quantileTDigestMerge(0.95)(e2e_state) AS e2e_p95_ms
FROM ${CLICKHOUSE_DB}.fact_event_latency_service_1m
WHERE bucket >= now() - INTERVAL ${WINDOW_MIN} MINUTE
GROUP BY bucket
ORDER BY bucket
FORMAT PrettyCompact
"

echo
echo "== sink/e2e p95 (stored_ts basis, last ${WINDOW_MIN}m) =="
run_query "
SELECT
  bucket,
  quantileTDigestMerge(0.95)(sink_state) AS sink_p95_ms,
  quantileTDigestMerge(0.95)(e2e_state) AS e2e_p95_ms
FROM ${CLICKHOUSE_DB}.fact_event_latency_1m
WHERE bucket >= now() - INTERVAL ${WINDOW_MIN} MINUTE
GROUP BY bucket
ORDER BY bucket
FORMAT PrettyCompact
"

echo
echo "== ingest->stored p95 (last ${WINDOW_MIN}m) =="
run_query "
SELECT
  bucket,
  quantileTDigestMerge(0.95)(ingest_state) AS ingest_to_stored_p95_ms
FROM ${CLICKHOUSE_DB}.fact_event_ingest_to_stored_1m
WHERE bucket >= now() - INTERVAL ${WINDOW_MIN} MINUTE
GROUP BY bucket
ORDER BY bucket
FORMAT PrettyCompact
"

echo
echo "== eps per minute (last ${WINDOW_MIN}m) =="
run_query "
SELECT
  bucket,
  uniqCombined64Merge(total_state) AS total,
  round(uniqCombined64Merge(total_state) / 60, 2) AS eps
FROM ${CLICKHOUSE_DB}.fact_event_agg_1m
WHERE bucket >= now() - INTERVAL ${WINDOW_MIN} MINUTE
GROUP BY bucket
ORDER BY bucket
FORMAT PrettyCompact
"

echo
echo "== top publish_p95 services (last ${WINDOW_MIN}m) =="
run_query "
SELECT
  service,
  quantileTDigestMerge(0.95)(publish_state) AS publish_p95_ms
FROM ${CLICKHOUSE_DB}.fact_event_latency_service_1m
WHERE bucket >= now() - INTERVAL ${WINDOW_MIN} MINUTE
GROUP BY service
ORDER BY publish_p95_ms DESC
LIMIT 5
FORMAT PrettyCompact
"
