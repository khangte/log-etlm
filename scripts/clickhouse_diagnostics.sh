#!/usr/bin/env bash
set -euo pipefail

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
echo "== 3) recent INSERT durations =="
run_query "SELECT event_time, query_duration_ms, written_rows, written_bytes \
FROM system.query_log \
WHERE type='QueryFinish' AND query LIKE 'INSERT%' \
ORDER BY event_time DESC LIMIT 20 FORMAT PrettyCompact"
