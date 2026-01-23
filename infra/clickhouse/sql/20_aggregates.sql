-- ============================================================================
-- Aggregate tables for Grafana (bucket basis, 1 minute)
-- ============================================================================

CREATE TABLE IF NOT EXISTS analytics.fact_event_agg_1m
(
    bucket       DateTime,
    service      LowCardinality(String),
    total_state  AggregateFunction(count, String),
    errors_state AggregateFunction(count, String)
)
ENGINE = AggregatingMergeTree
PARTITION BY toDate(bucket)
ORDER BY (bucket, service)
-- 기존: TTL bucket + INTERVAL 1 DAY (행 단위)
-- 변경: 파티션(일) 단위 TTL
TTL toDate(bucket) + INTERVAL 2 DAY;


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
TTL toDate(bucket) + INTERVAL 2 DAY;


CREATE TABLE IF NOT EXISTS analytics.fact_event_latency_1m
(
    bucket     DateTime,
    e2e_state  AggregateFunction(quantileTDigest, Float64),
    sink_state AggregateFunction(quantileTDigest, Float64),
    ingest_state AggregateFunction(quantileTDigest, Float64)
)
ENGINE = AggregatingMergeTree
PARTITION BY toDate(bucket)
ORDER BY bucket
TTL toDate(bucket) + INTERVAL 2 DAY;


CREATE TABLE IF NOT EXISTS analytics.fact_event_freshness_1m
(
    bucket           DateTime,
    max_ingest_state AggregateFunction(max, DateTime64(3))
)
ENGINE = AggregatingMergeTree
PARTITION BY toDate(bucket)
ORDER BY bucket
TTL toDate(bucket) + INTERVAL 2 DAY;


CREATE TABLE IF NOT EXISTS analytics.fact_event_latency_service_1m
(
    bucket        DateTime,
    service       LowCardinality(String),
    producer_to_kafka_state       AggregateFunction(quantileTDigest, Float64),
    kafka_to_spark_ingest_state   AggregateFunction(quantileTDigest, Float64),
    spark_processing_state        AggregateFunction(quantileTDigest, Float64),
    spark_to_stored_state         AggregateFunction(quantileTDigest, Float64),
    e2e_state     AggregateFunction(quantileTDigest, Float64)
)
ENGINE = AggregatingMergeTree
PARTITION BY toDate(bucket)
ORDER BY (bucket, service)
TTL toDate(bucket) + INTERVAL 2 DAY;


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
-- 기존: TTL bucket + INTERVAL 7 DAY (행 단위)
-- 변경: 파티션(일) 단위 TTL
TTL toDate(bucket) + INTERVAL 8 DAY;
