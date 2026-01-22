-- Materialized views for fact_event aggregates
CREATE MATERIALIZED VIEW IF NOT EXISTS analytics.mv_fact_event_agg_1m
TO analytics.fact_event_agg_1m
AS
SELECT
    toStartOfMinute(ingest_ts) AS bucket,
    service,
    uniqCombined64State(event_id) AS total_state,
    uniqCombined64StateIf(event_id, status_code >= 500) AS errors_state
FROM analytics.fact_event
GROUP BY bucket, service;

CREATE MATERIALIZED VIEW IF NOT EXISTS analytics.mv_fact_event_topic_1m
TO analytics.fact_event_topic_1m
AS
SELECT
    toStartOfMinute(ingest_ts) AS bucket,
    topic,
    uniqCombined64State(event_id) AS total_state
FROM analytics.fact_event
GROUP BY bucket, topic;

CREATE MATERIALIZED VIEW IF NOT EXISTS analytics.mv_fact_event_lag_1m
TO analytics.fact_event_lag_1m
AS
SELECT
    toStartOfMinute(ingest_ts) AS bucket,
    sum(toUInt64(greatest(dateDiff('second', event_ts, ingest_ts), 0))) AS sum_lag,
    count() AS cnt,
    countIf(dateDiff('second', event_ts, ingest_ts) < 0) AS skew_cnt
FROM analytics.fact_event
WHERE event_ts IS NOT NULL
  AND ingest_ts IS NOT NULL
GROUP BY bucket;

CREATE MATERIALIZED VIEW IF NOT EXISTS analytics.mv_fact_event_latency_1m
TO analytics.fact_event_latency_1m
AS
SELECT
    toStartOfMinute(stored_ts) AS bucket,
    quantileTDigestState(toFloat64(greatest(dateDiff('millisecond', event_ts, stored_ts), 0))) AS e2e_state,
    quantileTDigestState(toFloat64(greatest(dateDiff('millisecond', processed_ts, stored_ts), 0))) AS sink_state
FROM analytics.fact_event
WHERE event_ts IS NOT NULL
  AND processed_ts IS NOT NULL
  AND stored_ts IS NOT NULL
GROUP BY bucket;

-- ingest_ts -> stored_ts 지연 집계(1분)
CREATE MATERIALIZED VIEW IF NOT EXISTS analytics.mv_fact_event_ingest_to_stored_1m
TO analytics.fact_event_ingest_to_stored_1m
AS
SELECT
    toStartOfMinute(stored_ts) AS bucket,
    quantileTDigestState(toFloat64(greatest(dateDiff('millisecond', ingest_ts, stored_ts), 0))) AS ingest_state
FROM analytics.fact_event
WHERE ingest_ts IS NOT NULL
  AND stored_ts IS NOT NULL
GROUP BY bucket;

-- Freshness aggregates (ingest_ts max, 1분)
CREATE MATERIALIZED VIEW IF NOT EXISTS analytics.mv_fact_event_freshness_1m
TO analytics.fact_event_freshness_1m
AS
SELECT
    toStartOfMinute(ingest_ts) AS bucket,
    maxState(ingest_ts) AS max_ingest_state
FROM analytics.fact_event
WHERE ingest_ts IS NOT NULL
GROUP BY bucket;

-- Status code aggregates (1분)
CREATE MATERIALIZED VIEW IF NOT EXISTS analytics.mv_fact_event_status_code_1m
TO analytics.fact_event_status_code_1m
AS
SELECT
    toStartOfMinute(ingest_ts) AS bucket,
    status_code,
    count() AS cnt
FROM analytics.fact_event
WHERE ingest_ts IS NOT NULL
GROUP BY bucket, status_code;

CREATE MATERIALIZED VIEW IF NOT EXISTS analytics.mv_fact_event_latency_service_1m
TO analytics.fact_event_latency_service_1m
AS
SELECT
    toStartOfMinute(ingest_ts) AS bucket,
    service,
    quantileTDigestState(toFloat64(greatest(dateDiff('millisecond', event_ts, ingest_ts), 0))) AS queue_state,
    quantileTDigestState(toFloat64(greatest(dateDiff('millisecond', ingest_ts, processed_ts), 0))) AS publish_state,
    quantileTDigestState(
        toFloat64(greatest(dateDiff('millisecond', assumeNotNull(kafka_ts), processed_ts), 0))
    ) AS kafka_to_processed_state,
    quantileTDigestState(
        toFloat64(greatest(dateDiff('millisecond', ingest_ts, assumeNotNull(kafka_ts)), 0))
    ) AS ingest_to_kafka_state,
    quantileTDigestState(toFloat64(greatest(dateDiff('millisecond', event_ts, stored_ts), 0))) AS e2e_state
FROM analytics.fact_event
WHERE event_ts IS NOT NULL
  AND ingest_ts IS NOT NULL
  AND kafka_ts IS NOT NULL
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
