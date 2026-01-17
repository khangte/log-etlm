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

-- Topic EPS aggregates
CREATE TABLE IF NOT EXISTS analytics.fact_event_topic_1m
(
    bucket   DateTime,
    topic    LowCardinality(String),
    total_state AggregateFunction(uniqCombined64, String)
)
ENGINE = AggregatingMergeTree
PARTITION BY toDate(bucket)
ORDER BY (bucket, topic)
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

-- ingest_ts -> stored_ts 지연 집계 (1분)
CREATE TABLE IF NOT EXISTS analytics.fact_event_ingest_to_stored_1m
(
    bucket       DateTime,
    ingest_state AggregateFunction(quantileTDigest, Float64)
)
ENGINE = AggregatingMergeTree
PARTITION BY toDate(bucket)
ORDER BY bucket
TTL bucket + INTERVAL 1 DAY;

-- Service latency aggregates (ingest_ts/event_ts 기준)
CREATE TABLE IF NOT EXISTS analytics.fact_event_latency_service_1m
(
    bucket        DateTime,
    service       LowCardinality(String),
    queue_state   AggregateFunction(quantileTDigest, Float64),
    publish_state AggregateFunction(quantileTDigest, Float64),
    e2e_state     AggregateFunction(quantileTDigest, Float64)
)
ENGINE = AggregatingMergeTree
PARTITION BY toDate(bucket)
ORDER BY (bucket, service)
TTL bucket + INTERVAL 1 DAY;

-- 기존 테이블 컬럼 보강(이미 생성된 경우)
ALTER TABLE analytics.fact_event_latency_service_1m
    ADD COLUMN IF NOT EXISTS e2e_state AggregateFunction(quantileTDigest, Float64);

-- DLQ aggregates
CREATE TABLE IF NOT EXISTS analytics.fact_event_dlq_agg_1m
(
    bucket     DateTime,
    service    LowCardinality(String),
    error_type LowCardinality(String),
    total      UInt64
)
ENGINE = SummingMergeTree
PARTITION BY toDate(bucket)
ORDER BY (bucket, service, error_type)
TTL bucket + INTERVAL 7 DAY;
