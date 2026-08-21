#!/usr/bin/env bash
set -euo pipefail

# 용도: ClickHouse/Spark 진단 스크립트 통합. 섹션: ch / latency / parts / all.

CLICKHOUSE_CONTAINER="${CLICKHOUSE_CONTAINER:-clickhouse}"
CLICKHOUSE_USER="${CLICKHOUSE_USER:-log_user}"
CLICKHOUSE_PASSWORD="${CLICKHOUSE_PASSWORD:-log_pwd}"
CLICKHOUSE_DB="${CLICKHOUSE_DB:-analytics}"
WINDOW_MIN="${WINDOW_MIN:-10}"

usage() {
  echo "사용법: scripts/diag.sh <ch|latency|parts|all>"
  echo "  ch      ClickHouse merge/part/insert 상태 (5쿼리)"
  echo "  latency 구간별 p95, 분당 eps, publish_p95 상위 서비스 (3쿼리)"
  echo "  parts   Spark sink 로그 tail + part_log NewPart 분포"
  echo "  all     세 섹션 순차 실행"
}

run_query() {
  local sql="$1"
  docker exec -i "${CLICKHOUSE_CONTAINER}" clickhouse-client \
    -u "${CLICKHOUSE_USER}" --password "${CLICKHOUSE_PASSWORD}" \
    --query "${sql}"
}

section() {
  echo
  echo "== $1 =="
}

diag_ch() {
  section "1) merges (elapsed desc)"
  run_query "SELECT database, table, elapsed, progress, num_parts, result_part_name \
FROM system.merges ORDER BY elapsed DESC LIMIT 10 FORMAT PrettyCompact"

  section "2) active parts count"
  run_query "SELECT database, table, count() AS parts, formatReadableSize(sum(bytes_on_disk)) AS size \
FROM system.parts WHERE active GROUP BY database, table \
ORDER BY parts DESC LIMIT 10 FORMAT PrettyCompact"

  section "3) async insert log (real written rows/bytes)"
  run_query "SELECT
  event_time,
  database,
  table,
  format,
  rows,
  bytes,
  status,
  exception
FROM system.asynchronous_insert_log
WHERE event_time >= now() - INTERVAL 10 MINUTE
ORDER BY event_time DESC
LIMIT 20
FORMAT PrettyCompact"

  section "4) query_log INSERT (may show written_rows=0 when async_insert=1 & wait_for_async_insert=0)"
  run_query "SELECT
  event_time,
  query_duration_ms,
  written_rows,
  written_bytes,
  exception_code,
  exception
FROM system.query_log
WHERE type='QueryFinish'
  AND query_kind='Insert'
  AND event_time >= now() - INTERVAL 10 MINUTE
ORDER BY event_time DESC
LIMIT 20
FORMAT PrettyCompact"

  section "5) current async settings"
  run_query "SELECT
  getSetting('async_insert') AS async_insert,
  getSetting('wait_for_async_insert') AS wait_for_async_insert
FORMAT PrettyCompact"
}

diag_latency() {
  section "segmented p95 (producer/kafka/spark/sink, last ${WINDOW_MIN}m)"
  run_query "
SELECT
  bucket,
  quantileTDigestMerge(0.95)(producer_to_kafka_state) AS producer_to_kafka_p95_ms,
  quantileTDigestMerge(0.95)(kafka_to_spark_ingest_state) AS kafka_to_spark_p95_ms,
  quantileTDigestMerge(0.95)(spark_processing_state) AS spark_process_p95_ms,
  quantileTDigestMerge(0.95)(spark_to_stored_state) AS spark_to_stored_p95_ms,
  quantileTDigestMerge(0.95)(e2e_state) AS e2e_p95_ms
FROM ${CLICKHOUSE_DB}.event_log_latency_service_1m
WHERE bucket >= now() - INTERVAL ${WINDOW_MIN} MINUTE
GROUP BY bucket
ORDER BY bucket
FORMAT PrettyCompact
"

  section "eps per minute (last ${WINDOW_MIN}m)"
  run_query "
SELECT
  bucket,
  countMerge(total_state) AS total,
  round(countMerge(total_state) / 60, 2) AS eps
FROM ${CLICKHOUSE_DB}.event_log_agg_1m
WHERE bucket >= now() - INTERVAL ${WINDOW_MIN} MINUTE
GROUP BY bucket
ORDER BY bucket
FORMAT PrettyCompact
"

  section "top publish_p95 services (last ${WINDOW_MIN}m)"
  run_query "
SELECT
  service,
  quantileTDigestMerge(0.95)(producer_to_kafka_state) AS publish_p95_ms
FROM ${CLICKHOUSE_DB}.event_log_latency_service_1m
WHERE bucket >= now() - INTERVAL ${WINDOW_MIN} MINUTE
GROUP BY service
ORDER BY publish_p95_ms DESC
LIMIT 5
FORMAT PrettyCompact
"
}

diag_parts() {
  section "Spark sink partition logs (last 20 entries)"
  docker compose logs --tail 200 spark-driver | grep "clickhouse sink" | tail -n 20

  section "ClickHouse recent NewPart rows (last 5m)"
  echo "Event Time           Event_type Rows    Size in Bytes"
  run_query "
SELECT
    event_time,
    event_type,
    rows,
    size_in_bytes
FROM system.part_log
WHERE database='${CLICKHOUSE_DB}' AND table='event_log'
  AND event_type LIKE 'NewPart%'
  AND event_time >= now() - INTERVAL 5 MINUTE
ORDER BY event_time DESC
LIMIT 20
"
}

case "${1:-}" in
  ch)      diag_ch ;;
  latency) diag_latency ;;
  parts)   diag_parts ;;
  all)     diag_ch; diag_latency; diag_parts ;;
  *)       usage; exit 1 ;;
esac
