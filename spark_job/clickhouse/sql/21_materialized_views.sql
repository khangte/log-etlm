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

CREATE MATERIALIZED VIEW IF NOT EXISTS analytics.mv_fact_event_dlq_1m
TO analytics.fact_event_dlq_1m
AS
SELECT
    toStartOfMinute(ingest_ts) AS bucket,
    coalesce(source_topic, 'unknown') AS fail_stage,
    error_type AS fail_reason,
    count() AS cnt
FROM analytics.fact_event_dlq
GROUP BY bucket, fail_stage, fail_reason;
