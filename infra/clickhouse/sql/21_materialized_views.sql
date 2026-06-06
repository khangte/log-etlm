-- ============================================================================
-- Materialized views for event_log aggregates (lighter version)
-- - WITH로 time diff 계산을 재사용해 함수 호출 수 절감
-- ============================================================================

CREATE MATERIALIZED VIEW IF NOT EXISTS analytics.mv_event_log_agg_1m
TO analytics.event_log_agg_1m
AS
SELECT
    toStartOfMinute(kafka_received_at) AS bucket,
    service,
    countState(event_id) AS total_state,
    countStateIf(event_id, status_code >= 500) AS errors_state
FROM analytics.event_log
GROUP BY bucket, service;

CREATE MATERIALIZED VIEW IF NOT EXISTS analytics.mv_event_log_created_stored_1m
TO analytics.event_log_created_stored_1m
AS
SELECT
    toStartOfMinute(event_timestamp) AS bucket,
    count() AS created_cnt,
    toUInt64(0) AS stored_cnt
FROM analytics.event_log
WHERE event_timestamp IS NOT NULL
GROUP BY bucket
UNION ALL
SELECT
    toStartOfMinute(clickhouse_stored_at) AS bucket,
    toUInt64(0) AS created_cnt,
    count() AS stored_cnt
FROM analytics.event_log
WHERE clickhouse_stored_at IS NOT NULL
GROUP BY bucket;


CREATE MATERIALIZED VIEW IF NOT EXISTS analytics.mv_event_log_lag_1m
TO analytics.event_log_lag_1m
AS
WITH
    dateDiff('second', event_timestamp, kafka_received_at) AS lag_s
SELECT
    toStartOfMinute(kafka_received_at) AS bucket,
    sum(toUInt64(greatest(lag_s, 0))) AS sum_lag,
    count() AS cnt,
    countIf(lag_s < 0) AS skew_cnt
FROM analytics.event_log
WHERE event_timestamp IS NOT NULL
  AND kafka_received_at IS NOT NULL
GROUP BY bucket;


-- mv_event_log_latency_1m 제거됨 (2026-05-20):
-- mv_event_log_latency_service_1m이 동일 컬럼(e2e_state, spark_processing_state 등)을
-- service 차원 포함하여 제공하므로 중복. Grafana 쿼리는 event_log_latency_service_1m으로 이전.

-- 이 MV는 가장 CPU가 비싼 편. 중복 제거 + WITH로 diff 재사용.
CREATE MATERIALIZED VIEW IF NOT EXISTS analytics.mv_event_log_latency_service_1m
TO analytics.event_log_latency_service_1m
AS
WITH
    toStartOfMinute(kafka_received_at) AS bucket,
    assumeNotNull(event_timestamp) AS e_ts,
    assumeNotNull(kafka_received_at) AS k_ts,
    assumeNotNull(spark_received_at) AS s_ts,
    dateDiff('millisecond', e_ts, k_ts) AS producer_to_kafka_ms,
    dateDiff('millisecond', k_ts, s_ts) AS kafka_to_spark_ms,
    dateDiff('millisecond', s_ts, spark_processed_at) AS spark_processing_ms,
    dateDiff('millisecond', spark_processed_at, clickhouse_stored_at) AS spark_to_stored_ms,
    dateDiff('millisecond', k_ts, clickhouse_stored_at) AS e2e_ms

SELECT
    bucket,
    service,
    quantileTDigestState(toFloat64(greatest(producer_to_kafka_ms, 0))) AS producer_to_kafka_state,
    quantileTDigestState(toFloat64(greatest(kafka_to_spark_ms, 0))) AS kafka_to_spark_ingest_state,
    quantileTDigestState(toFloat64(greatest(spark_processing_ms, 0))) AS spark_processing_state,
    quantileTDigestState(toFloat64(greatest(spark_to_stored_ms, 0))) AS spark_to_stored_state,
    quantileTDigestState(toFloat64(greatest(e2e_ms, 0))) AS e2e_state,
    maxState(k_ts) AS max_ingest_state
FROM analytics.event_log
WHERE event_timestamp IS NOT NULL
  AND kafka_received_at IS NOT NULL
  AND spark_received_at IS NOT NULL
  AND spark_processed_at IS NOT NULL
  AND clickhouse_stored_at IS NOT NULL
GROUP BY bucket, service;


CREATE MATERIALIZED VIEW IF NOT EXISTS analytics.mv_event_log_dlq_agg_1m
TO analytics.event_log_dlq_agg_1m
AS
SELECT
    toStartOfMinute(kafka_received_at) AS bucket,
    coalesce(service, 'unknown') AS service,
    coalesce(error_type, 'unknown') AS error_type,
    count() AS total
FROM analytics.event_log_dlq
GROUP BY bucket, service, error_type;


-- ============================================================================
-- Realtime materialized views (10 second buckets)
-- ============================================================================

CREATE MATERIALIZED VIEW IF NOT EXISTS analytics.mv_event_log_agg_10s
TO analytics.event_log_agg_10s
AS
SELECT
    toStartOfInterval(kafka_received_at, INTERVAL 10 second) AS bucket,
    countState(event_id) AS total_state,
    countStateIf(event_id, status_code >= 500) AS errors_state
FROM analytics.event_log
GROUP BY bucket;


-- mv_event_log_latency_10s 제거됨 (2026-05-20):
-- mv_event_log_latency_stage_10s가 e2e_state를 포함하여 제공하므로 중복.
-- Grafana 쿼리는 event_log_latency_stage_10s로 이전.


-- event_timestamp 기준 단계별 지연, bucket은 kafka_received_at 10초 기준
CREATE MATERIALIZED VIEW IF NOT EXISTS analytics.mv_event_log_latency_stage_10s
TO analytics.event_log_latency_stage_10s
AS
WITH
    toStartOfInterval(kafka_received_at, INTERVAL 10 second) AS bucket,
    assumeNotNull(event_timestamp) AS e_ts,
    assumeNotNull(kafka_received_at) AS k_ts,
    assumeNotNull(spark_received_at) AS s_ts,
    dateDiff('millisecond', e_ts, k_ts) AS producer_to_kafka_ms,
    dateDiff('millisecond', k_ts, s_ts) AS kafka_to_spark_ms,
    dateDiff('millisecond', s_ts, spark_processed_at) AS spark_processing_ms,
    dateDiff('millisecond', spark_processed_at, clickhouse_stored_at) AS spark_to_stored_ms,
    dateDiff('millisecond', k_ts, clickhouse_stored_at) AS e2e_ms
SELECT
    bucket,
    quantileTDigestState(toFloat64(greatest(producer_to_kafka_ms, 0))) AS producer_to_kafka_state,
    quantileTDigestState(toFloat64(greatest(kafka_to_spark_ms, 0))) AS kafka_to_spark_ingest_state,
    quantileTDigestState(toFloat64(greatest(spark_processing_ms, 0))) AS spark_processing_state,
    quantileTDigestState(toFloat64(greatest(spark_to_stored_ms, 0))) AS spark_to_stored_state,
    quantileTDigestState(toFloat64(greatest(e2e_ms, 0))) AS e2e_state
FROM analytics.event_log
WHERE event_timestamp IS NOT NULL
  AND kafka_received_at IS NOT NULL
  AND spark_received_at IS NOT NULL
  AND spark_processed_at IS NOT NULL
  AND clickhouse_stored_at IS NOT NULL
GROUP BY bucket;
