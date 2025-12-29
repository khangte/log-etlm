#!/usr/bin/env bash
# ClickHouse SQL로 dim 테이블을 배치 갱신한다.
# DIM_LOOKBACK_DAYS로 범위를 제한하고, DIM_RESET=1이면 TRUNCATE 후 재적재한다.
set -euo pipefail
cd /home/kang/log-etlm

LOOKBACK_DAYS="${DIM_LOOKBACK_DAYS:-1}"
RESET="${DIM_RESET:-1}"

if [ "$RESET" = "1" ]; then
  docker exec -i clickhouse clickhouse-client -u log_user --password log_pwd --multiquery <<'SQL'
TRUNCATE TABLE analytics.dim_date;
TRUNCATE TABLE analytics.dim_time;
TRUNCATE TABLE analytics.dim_service;
TRUNCATE TABLE analytics.dim_status_code;
TRUNCATE TABLE analytics.dim_user;
SQL
fi

docker exec -i clickhouse clickhouse-client -u log_user --password log_pwd --multiquery <<SQL
INSERT INTO analytics.dim_date
SELECT
  date,
  toYear(date) AS year,
  toMonth(date) AS month,
  toDayOfMonth(date) AS day,
  toWeek(date) AS week,
  toDayOfWeek(date) AS day_of_week,
  if(day_of_week IN (6, 7), 1, 0) AS is_weekend
FROM (
  SELECT toDate(event_ts) AS date
  FROM analytics.fact_log
  WHERE event_ts >= now() - INTERVAL ${LOOKBACK_DAYS} DAY
  GROUP BY date
);

INSERT INTO analytics.dim_time
SELECT
  toUInt32(hour * 10000 + minute * 100 + second) AS time_key,
  hour,
  minute,
  second,
  multiIf(hour BETWEEN 0 AND 5, 'dawn',
          hour BETWEEN 6 AND 11, 'morning',
          hour BETWEEN 12 AND 17, 'afternoon',
          'evening') AS time_of_day
FROM (
  SELECT
    toHour(event_ts) AS hour,
    toMinute(event_ts) AS minute,
    toSecond(event_ts) AS second
  FROM analytics.fact_log
  WHERE event_ts >= now() - INTERVAL ${LOOKBACK_DAYS} DAY
  GROUP BY hour, minute, second
);

INSERT INTO analytics.dim_service
SELECT
  service,
  'default' AS service_group,
  1 AS is_active,
  cast(NULL AS Nullable(String)) AS description
FROM (
  SELECT service
  FROM analytics.fact_log
  WHERE event_ts >= now() - INTERVAL ${LOOKBACK_DAYS} DAY
    AND service IS NOT NULL
  GROUP BY service
);

INSERT INTO analytics.dim_status_code
SELECT
  status_code,
  concat(toString(intDiv(status_code, 100)), 'xx') AS status_class,
  if(status_code BETWEEN 500 AND 599, 1, 0) AS is_error,
  multiIf(status_code = 200, 'OK',
          status_code = 201, 'Created',
          status_code = 204, 'No Content',
          status_code = 400, 'Bad Request',
          status_code = 401, 'Unauthorized',
          status_code = 403, 'Forbidden',
          status_code = 404, 'Not Found',
          status_code = 422, 'Unprocessable Entity',
          status_code = 429, 'Too Many Requests',
          status_code = 500, 'Internal Server Error',
          status_code = 502, 'Bad Gateway',
          status_code = 503, 'Service Unavailable',
          'Unknown') AS description
FROM (
  SELECT status_code
  FROM analytics.fact_log
  WHERE event_ts >= now() - INTERVAL ${LOOKBACK_DAYS} DAY
    AND status_code IS NOT NULL
  GROUP BY status_code
);

INSERT INTO analytics.dim_user
SELECT
  user_id,
  1 AS is_active,
  cast(NULL AS Nullable(String)) AS description
FROM (
  SELECT user_id
  FROM analytics.fact_log
  WHERE event_ts >= now() - INTERVAL ${LOOKBACK_DAYS} DAY
    AND user_id IS NOT NULL
  GROUP BY user_id
);
SQL
