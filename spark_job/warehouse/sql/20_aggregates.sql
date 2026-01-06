-- Aggregate tables for Grafana (ingest_ts basis)
CREATE TABLE IF NOT EXISTS analytics.fact_event_agg_1m
(
    bucket       DateTime,
    service      LowCardinality(String),
    total_state  AggregateFunction(uniqCombined64, String),
    errors_state AggregateFunction(uniqCombined64, String)
)
ENGINE = AggregatingMergeTree
PARTITION BY toDate(bucket)
ORDER BY (bucket, service)
TTL bucket + INTERVAL 1 DAY;

-- Lag aggregates (ingest_ts basis)
CREATE TABLE IF NOT EXISTS analytics.fact_event_lag_1m
(
    bucket   DateTime,
    sum_lag  UInt64,
    cnt      UInt64,
    skew_cnt UInt64
)
ENGINE = SummingMergeTree
PARTITION BY toDate(bucket)
ORDER BY bucket
TTL bucket + INTERVAL 1 DAY;

-- Latency aggregates (stored_ts basis)
CREATE TABLE IF NOT EXISTS analytics.fact_event_latency_1m
(
    bucket    DateTime,
    e2e_state AggregateFunction(quantileTDigest, Float64),
    sink_state AggregateFunction(quantileTDigest, Float64)
)
ENGINE = AggregatingMergeTree
PARTITION BY toDate(bucket)
ORDER BY bucket
TTL bucket + INTERVAL 1 DAY;
