#!/bin/bash
set -euo pipefail

# 용도: Spark ClickHouse sink 파티션 로그와 ClickHouse 신규 파트 로우 분포를 함께 보여줘서 spike 원인 파악에 도움을 줌.

echo ">>> Spark sink partition logs (last 20 entries) <<<"
docker compose logs --tail 200 spark-driver | grep "clickhouse sink" | tail -n 20

echo
echo ">>> ClickHouse recent NewPart rows (last 5m) <<<"
docker exec -it clickhouse clickhouse-client -u log_user --password log_pwd --query "
SELECT
    event_time,
    event_type,
    rows,
    size_in_bytes
FROM system.part_log
WHERE database='analytics' AND table='fact_event'
  AND event_type LIKE 'NewPart%'
  AND event_time >= now() - INTERVAL 5 MINUTE
ORDER BY event_time DESC
LIMIT 20
"
