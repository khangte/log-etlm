#!/usr/bin/env bash
set -euo pipefail

# 용도: ClickHouse merge/part/insert 상태를 빠르게 확인하는 진단 스크립트.

CLICKHOUSE_CONTAINER="${CLICKHOUSE_CONTAINER:-clickhouse}"
CLICKHOUSE_USER="${CLICKHOUSE_USER:-log_user}"
CLICKHOUSE_PASSWORD="${CLICKHOUSE_PASSWORD:-log_pwd}"

run_query() {
  local sql="$1"
  docker exec -it "${CLICKHOUSE_CONTAINER}" clickhouse-client \
    -u "${CLICKHOUSE_USER}" --password "${CLICKHOUSE_PASSWORD}" \
    --query "${sql}"
}

echo "== 1) merges (elapsed desc) =="
run_query "SELECT database, table, elapsed, progress, num_parts, result_part_name \
FROM system.merges ORDER BY elapsed DESC LIMIT 10 FORMAT PrettyCompact"

echo
echo "== 2) active parts count =="
run_query "SELECT database, table, count() AS parts, formatReadableSize(sum(bytes_on_disk)) AS size \
FROM system.parts WHERE active GROUP BY database, table \
ORDER BY parts DESC LIMIT 10 FORMAT PrettyCompact"

echo
echo "== 3) async insert log (real written rows/bytes) =="
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

echo
echo "== 4) query_log INSERT (may show written_rows=0 when async_insert=1 & wait_for_async_insert=0) =="
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

echo
echo "== 5) current async settings =="
run_query "SELECT
  getSetting('async_insert') AS async_insert,
  getSetting('wait_for_async_insert') AS wait_for_async_insert
FORMAT PrettyCompact"
