CREATE DATABASE IF NOT EXISTS analytics;

-- 메인 이벤트 로그 테이블 (원본 로그 적재용)
CREATE TABLE IF NOT EXISTS analytics.fact_log
(
    -- 시간
    event_ts     DateTime64(3)           ,  -- 발생 시각 (UTC, Spark에서 변환)
    ingest_ts    DateTime64(3)           ,  -- Kafka 적재 시각 (UTC)
    processed_ts DateTime64(3)           ,  -- Spark 처리 시각 (UTC)
    stored_ts    DateTime64(3) DEFAULT now64(3),  -- ClickHouse 적재 시각 (UTC)

    -- 공통 메타 정보
    service        LowCardinality(String),
    level          String,
    request_id     String,
    method         String,
    path           String,
    status_code    Int32,
    event          String,

    -- 비즈니스 필드
    user_id           Nullable(String),
    notification_type Nullable(String),
    product_id        Nullable(Int32),
    amount            Nullable(Int32),

    -- 인프라/추적용
    topic     String,       -- Kafka topic 이름

    -- 원본 JSON 백업
    raw_json  String
)
ENGINE = MergeTree
PARTITION BY toDate(event_ts)
ORDER BY (service, event_ts)
TTL event_ts + INTERVAL 1 DAY;

-- ---------------------------------------------------------------------------
-- Grafana용 1분 집계 테이블 (원본 fact_log를 매번 스캔하면 SELECT가 수십~수백초까지 늘어나
-- ingest(Spark→ClickHouse INSERT)까지 같이 느려질 수 있음.
-- materialized view로 집계를 유지하고, Grafana는 이 테이블만 조회하도록 한다.
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS analytics.fact_log_agg_1m
(
    bucket       DateTime,
    service      LowCardinality(String),
    total        UInt64,
    errors       UInt64
)
ENGINE = SummingMergeTree
PARTITION BY toDate(bucket)
ORDER BY (bucket, service)
TTL bucket + INTERVAL 1 DAY;


CREATE MATERIALIZED VIEW IF NOT EXISTS analytics.mv_fact_log_agg_1m
TO analytics.fact_log_agg_1m
AS
SELECT
    -- Grafana에서 "현재 EPS"를 보고 싶으면 event_ts(이벤트 시간)보다 ingest_ts(적재 시간)가 더 정확하다.
    -- event_ts가 과거/미래로 스큐되면 최근 1분 EPS가 낮게 보일 수 있음.
    toStartOfMinute(ingest_ts) AS bucket,
    service,
    count() AS total,
    countIf(status_code >= 500) AS errors
FROM analytics.fact_log
GROUP BY bucket, service;

-- ---------------------------------------------------------------------------
-- Grafana용 1분 집계 테이블 (event_ts 기준)
-- "로그 생성 시각(event_ts) 기준 EPS"가 필요할 때 사용한다.
-- NOTE: event_ts가 스큐/지연되면 버킷 분포가 달라질 수 있다.
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS analytics.fact_log_agg_event_1m
(
    bucket       DateTime,
    service      LowCardinality(String),
    total        UInt64,
    errors       UInt64
)
ENGINE = SummingMergeTree
PARTITION BY toDate(bucket)
ORDER BY (bucket, service)
TTL bucket + INTERVAL 1 DAY;


CREATE MATERIALIZED VIEW IF NOT EXISTS analytics.mv_fact_log_agg_event_1m
TO analytics.fact_log_agg_event_1m
AS
SELECT
    toStartOfMinute(event_ts) AS bucket,
    service,
    count() AS total,
    countIf(status_code >= 500) AS errors
FROM analytics.fact_log
GROUP BY bucket, service;

-- ---------------------------------------------------------------------------
-- Kafka 토픽별 EPS 집계 테이블 (ingest_ts 기준 1분 버킷)
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS analytics.fact_log_topic_1m
(
    bucket   DateTime,
    topic    LowCardinality(String),
    total    UInt64
)
ENGINE = SummingMergeTree
PARTITION BY toDate(bucket)
ORDER BY (bucket, topic)
TTL bucket + INTERVAL 1 DAY;

CREATE MATERIALIZED VIEW IF NOT EXISTS analytics.mv_fact_log_topic_1m
TO analytics.fact_log_topic_1m
AS
SELECT
    toStartOfMinute(ingest_ts) AS bucket,
    topic,
    count() AS total
FROM analytics.fact_log
GROUP BY bucket, topic;

-- ---------------------------------------------------------------------------
-- Spark 처리 지연(avg) 집계 테이블 (ingest_ts 기준 1분 버킷)
-- - fact_log 풀스캔을 피하기 위해 sum/count로 평균 계산
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS analytics.fact_log_lag_1m
(
    bucket   DateTime,
    sum_lag  UInt64,
    cnt      UInt64
)
ENGINE = SummingMergeTree
PARTITION BY toDate(bucket)
ORDER BY bucket
TTL bucket + INTERVAL 1 DAY;

CREATE MATERIALIZED VIEW IF NOT EXISTS analytics.mv_fact_log_lag_1m
TO analytics.fact_log_lag_1m
AS
SELECT
    toStartOfMinute(ingest_ts) AS bucket,
    sum(toUInt64(dateDiff('second', event_ts, ingest_ts))) AS sum_lag,
    count() AS cnt
FROM analytics.fact_log
WHERE event_ts IS NOT NULL
  AND ingest_ts IS NOT NULL
GROUP BY bucket;

-- ---------------------------------------------------------------------------
-- End-to-end/구간 지연 p95 집계 테이블 (stored_ts 기준 1분 버킷)
-- - 원본 fact_log 전체 스캔을 피하기 위해 AggregatingMergeTree 사용
-- - Grafana는 이 테이블을 조회하도록 한다.
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS analytics.fact_log_latency_1m
(
    bucket       DateTime,
    e2e_state    AggregateFunction(quantileTDigest, Float64),
    ingest_state AggregateFunction(quantileTDigest, Float64),
    process_state AggregateFunction(quantileTDigest, Float64),
    sink_state   AggregateFunction(quantileTDigest, Float64)
)
ENGINE = AggregatingMergeTree
PARTITION BY toDate(bucket)
ORDER BY bucket
TTL bucket + INTERVAL 1 DAY;

CREATE MATERIALIZED VIEW IF NOT EXISTS analytics.mv_fact_log_latency_1m
TO analytics.fact_log_latency_1m
AS
SELECT
    toStartOfMinute(stored_ts) AS bucket,
    quantileTDigestState(toFloat64(dateDiff('millisecond', event_ts, stored_ts))) AS e2e_state,
    quantileTDigestState(toFloat64(dateDiff('millisecond', event_ts, ingest_ts))) AS ingest_state,
    quantileTDigestState(toFloat64(dateDiff('millisecond', ingest_ts, processed_ts))) AS process_state,
    quantileTDigestState(toFloat64(dateDiff('millisecond', processed_ts, stored_ts))) AS sink_state
FROM analytics.fact_log
WHERE event_ts IS NOT NULL
  AND ingest_ts IS NOT NULL
  AND processed_ts IS NOT NULL
  AND stored_ts IS NOT NULL
GROUP BY bucket;


-- ---------------------------------------------------------------------------
-- Dimension 테이블 (최소 구성)
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS analytics.dim_service
(
    service       String,
    service_group String,
    is_active     UInt8,
    description   String
)
ENGINE = MergeTree
ORDER BY service;


CREATE TABLE IF NOT EXISTS analytics.dim_status_code
(
    status_code  Int32,
    status_class String,
    is_error     UInt8,
    description  String
)
ENGINE = MergeTree
ORDER BY status_code;

CREATE TABLE IF NOT EXISTS analytics.dim_date
(
    date         Date,
    year         UInt16,
    month        UInt8,
    day          UInt8,
    week         UInt8,
    day_of_week  UInt8,
    is_weekend   UInt8
)
ENGINE = MergeTree
ORDER BY date;


CREATE TABLE IF NOT EXISTS analytics.dim_time
(
    time_key    UInt32,   -- HHMMSS 형식 (예: 093015)
    hour        UInt8,    -- 0~23
    minute      UInt8,    -- 0~59
    second      UInt8,    -- 0~59
    time_of_day String    -- dawn/morning/afternoon/evening
)
ENGINE = MergeTree
ORDER BY time_key;
