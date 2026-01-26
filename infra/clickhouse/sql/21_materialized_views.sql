-- ============================================================================
-- Materialized views for fact_event aggregates (lighter version)
-- - WITH로 time diff 계산을 재사용해 함수 호출 수 절감
-- ============================================================================

CREATE MATERIALIZED VIEW IF NOT EXISTS analytics.mv_fact_event_agg_1m
TO analytics.fact_event_agg_1m
AS
SELECT
    toStartOfMinute(ingest_ts) AS bucket,
    service,
    countState(event_id) AS total_state,
    countStateIf(event_id, status_code >= 500) AS errors_state
FROM analytics.fact_event
GROUP BY bucket, service;


CREATE MATERIALIZED VIEW IF NOT EXISTS analytics.mv_fact_event_lag_1m
TO analytics.fact_event_lag_1m
AS
WITH
    dateDiff('second', event_ts, ingest_ts) AS lag_s
SELECT
    toStartOfMinute(ingest_ts) AS bucket,
    sum(toUInt64(greatest(lag_s, 0))) AS sum_lag,
    count() AS cnt,
    countIf(lag_s < 0) AS skew_cnt
FROM analytics.fact_event
WHERE event_ts IS NOT NULL
  AND ingest_ts IS NOT NULL
GROUP BY bucket;


CREATE MATERIALIZED VIEW IF NOT EXISTS analytics.mv_fact_event_latency_1m
TO analytics.fact_event_latency_1m
AS
WITH
    dateDiff('millisecond', event_ts, stored_ts) AS e2e_ms,
    dateDiff('millisecond', processed_ts, stored_ts) AS sink_ms,
    dateDiff('millisecond', ingest_ts, stored_ts) AS ingest_ms,
    dateDiff('millisecond', spark_ingest_ts, processed_ts) AS spark_processing_ms
SELECT
    toStartOfMinute(ingest_ts) AS bucket,
    quantileTDigestState(toFloat64(greatest(e2e_ms, 0))) AS e2e_state,
    quantileTDigestState(toFloat64(greatest(sink_ms, 0))) AS sink_state,
    quantileTDigestState(toFloat64(greatest(ingest_ms, 0))) AS ingest_state,
    quantileTDigestState(toFloat64(greatest(ifNull(spark_processing_ms, 0), 0))) AS spark_processing_state
FROM analytics.fact_event
WHERE event_ts IS NOT NULL
  AND ingest_ts IS NOT NULL
  AND spark_ingest_ts IS NOT NULL
  AND processed_ts IS NOT NULL
  AND stored_ts IS NOT NULL
GROUP BY bucket;

CREATE MATERIALIZED VIEW IF NOT EXISTS analytics.mv_fact_event_freshness_1m
TO analytics.fact_event_freshness_1m
AS
SELECT
    toStartOfMinute(ingest_ts) AS bucket,
    maxState(ingest_ts) AS max_ingest_state
FROM analytics.fact_event
WHERE ingest_ts IS NOT NULL
GROUP BY bucket;


-- ⚠️ 이 MV는 가장 CPU가 비싼 편. 중복 제거 + WITH로 diff 재사용.
CREATE MATERIALIZED VIEW IF NOT EXISTS analytics.mv_fact_event_latency_service_1m
TO analytics.fact_event_latency_service_1m
AS
WITH
    -- bucket을 ingest_ts 대신 created_ts 기준으로 변경
    toStartOfMinute(assumeNotNull(created_ts)) AS bucket,

    -- null 제거 후 쓰려고 where로 강제했으니 assumeNotNull은 필수는 아니지만
    -- 타입 때문에 남겨야 하면 유지.
    assumeNotNull(created_ts) AS c_ts,
    assumeNotNull(ingest_ts) AS i_ts,
    assumeNotNull(spark_ingest_ts) AS s_ingest,
    -- E2E 관련 지연 시간 계산 (created_ts 기준)
    dateDiff('millisecond', c_ts, i_ts) AS producer_to_kafka_ms,    -- Producer 생성 ~ Kafka 수집
    dateDiff('millisecond', i_ts, s_ingest) AS kafka_to_spark_ingest_ms, -- Kafka 수집 ~ Spark 수집
    dateDiff('millisecond', s_ingest, processed_ts) AS spark_processing_ms, -- Spark 수집 ~ Spark 처리 완료
    dateDiff('millisecond', processed_ts, stored_ts) AS spark_to_stored_ms,   -- Spark 처리 완료 ~ ClickHouse 적재
    dateDiff('millisecond', c_ts, stored_ts) AS e2e_ms               -- Producer 생성 ~ ClickHouse 적재 (새로운 E2E)

SELECT
    bucket,
    service,
    quantileTDigestState(toFloat64(greatest(producer_to_kafka_ms, 0))) AS producer_to_kafka_state,
    quantileTDigestState(toFloat64(greatest(kafka_to_spark_ingest_ms, 0))) AS kafka_to_spark_ingest_state,
    quantileTDigestState(toFloat64(greatest(spark_processing_ms, 0))) AS spark_processing_state,
    quantileTDigestState(toFloat64(greatest(spark_to_stored_ms, 0))) AS spark_to_stored_state,
    quantileTDigestState(toFloat64(greatest(e2e_ms, 0))) AS e2e_state
FROM analytics.fact_event
WHERE created_ts IS NOT NULL
  AND ingest_ts IS NOT NULL
  AND spark_ingest_ts IS NOT NULL
  AND processed_ts IS NOT NULL
  AND stored_ts IS NOT NULL
GROUP BY bucket, service;


CREATE MATERIALIZED VIEW IF NOT EXISTS analytics.mv_fact_event_dlq_agg_1m
TO analytics.fact_event_dlq_agg_1m
AS
SELECT
    toStartOfMinute(ingest_ts) AS bucket,
    coalesce(service, 'unknown') AS service,
    coalesce(error_type, 'unknown') AS error_type,
    count() AS total
FROM analytics.fact_event_dlq
GROUP BY bucket, service, error_type;


-- ============================================================================
-- Realtime materialized views (10 second buckets)
-- ============================================================================

CREATE MATERIALIZED VIEW IF NOT EXISTS analytics.mv_fact_event_agg_10s
TO analytics.fact_event_agg_10s
AS
SELECT
    toStartOfInterval(ingest_ts, INTERVAL 10 second) AS bucket,
    countState(event_id) AS total_state,
    countStateIf(event_id, status_code >= 500) AS errors_state
FROM analytics.fact_event
GROUP BY bucket;


CREATE MATERIALIZED VIEW IF NOT EXISTS analytics.mv_fact_event_latency_10s
TO analytics.fact_event_latency_10s
AS
WITH
    dateDiff('millisecond', event_ts, stored_ts) AS e2e_ms,
    dateDiff('millisecond', processed_ts, stored_ts) AS sink_ms,
    dateDiff('millisecond', ingest_ts, stored_ts) AS ingest_ms,
    dateDiff('millisecond', spark_ingest_ts, processed_ts) AS spark_processing_ms
SELECT
    toStartOfInterval(ingest_ts, INTERVAL 10 second) AS bucket,
    quantileTDigestState(toFloat64(greatest(e2e_ms, 0))) AS e2e_state,
    quantileTDigestState(toFloat64(greatest(sink_ms, 0))) AS sink_state,
    quantileTDigestState(toFloat64(greatest(ingest_ms, 0))) AS ingest_state,
    quantileTDigestState(toFloat64(greatest(ifNull(spark_processing_ms, 0), 0))) AS spark_processing_state
FROM analytics.fact_event
WHERE event_ts IS NOT NULL
  AND ingest_ts IS NOT NULL
  AND spark_ingest_ts IS NOT NULL
  AND processed_ts IS NOT NULL
  AND stored_ts IS NOT NULL
GROUP BY bucket;


-- created_ts 기준 단계별 지연. bucket은 ingest_ts 10초 기준
CREATE MATERIALIZED VIEW IF NOT EXISTS analytics.mv_fact_event_latency_stage_10s
TO analytics.fact_event_latency_stage_10s
AS
WITH
    toStartOfInterval(ingest_ts, INTERVAL 10 second) AS bucket,
    assumeNotNull(created_ts) AS c_ts,
    assumeNotNull(ingest_ts) AS i_ts,
    assumeNotNull(spark_ingest_ts) AS s_ingest,
    dateDiff('millisecond', c_ts, i_ts) AS producer_to_kafka_ms,
    dateDiff('millisecond', i_ts, s_ingest) AS kafka_to_spark_ingest_ms,
    dateDiff('millisecond', s_ingest, processed_ts) AS spark_processing_ms,
    dateDiff('millisecond', processed_ts, stored_ts) AS spark_to_stored_ms,
    dateDiff('millisecond', c_ts, stored_ts) AS e2e_ms
SELECT
    bucket,
    quantileTDigestState(toFloat64(greatest(producer_to_kafka_ms, 0))) AS producer_to_kafka_state,
    quantileTDigestState(toFloat64(greatest(kafka_to_spark_ingest_ms, 0))) AS kafka_to_spark_ingest_state,
    quantileTDigestState(toFloat64(greatest(spark_processing_ms, 0))) AS spark_processing_state,
    quantileTDigestState(toFloat64(greatest(spark_to_stored_ms, 0))) AS spark_to_stored_state,
    quantileTDigestState(toFloat64(greatest(e2e_ms, 0))) AS e2e_state
FROM analytics.fact_event
WHERE created_ts IS NOT NULL
  AND ingest_ts IS NOT NULL
  AND spark_ingest_ts IS NOT NULL
  AND processed_ts IS NOT NULL
  AND stored_ts IS NOT NULL
GROUP BY bucket;
