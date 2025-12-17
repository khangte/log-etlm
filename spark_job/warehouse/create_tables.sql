CREATE DATABASE IF NOT EXISTS analytics;

CREATE TABLE IF NOT EXISTS analytics.fact_log
(
    -- 시간
    -- ClickHouse JDBC 0.4.x가 DateTime64(precision, 'TZ')를 TIMESTAMP_WITH_TIMEZONE으로 반환하면서
    -- Spark 4.0이 타입을 파싱하지 못하므로 타임존 표기를 생략하고 UTC 기준으로 저장한다.
    event_ts   DateTime64(3)           ,  -- UTC 보장 (Spark에서 변환)
    ingest_ts  DateTime64(3)           ,  -- UTC 보장 (Spark에서 변환)

    -- 공통 메타 정보
    service        LowCardinality(String),
    level          String,
    request_id     String,
    method         String,
    path           String,
    status_code    Int32,
    latency        Float64,
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
ORDER BY (service, event_ts, request_id);

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
    errors       UInt64,
    sum_latency  Float64
)
ENGINE = SummingMergeTree
PARTITION BY toDate(bucket)
ORDER BY (bucket, service);


CREATE MATERIALIZED VIEW IF NOT EXISTS analytics.mv_fact_log_agg_1m
TO analytics.fact_log_agg_1m
AS
SELECT
    -- Grafana에서 "현재 RPS"를 보고 싶으면 event_ts(이벤트 시간)보다 ingest_ts(적재 시간)가 더 정확하다.
    -- event_ts가 과거/미래로 스큐되면 최근 1분 RPS가 낮게 보일 수 있음.
    toStartOfMinute(ingest_ts) AS bucket,
    service,
    count() AS total,
    countIf(status_code >= 500) AS errors,
    sum(latency) AS sum_latency
FROM analytics.fact_log
GROUP BY bucket, service;


-- CREATE TABLE IF NOT EXISTS analytics.dim_service
-- (
--     service       String,
--     service_group String,
--     is_active     UInt8
-- )
-- ENGINE = MergeTree
-- ORDER BY service;


-- CREATE TABLE IF NOT EXISTS analytics.dim_status_code
-- (
--     status_code  Int32,
--     status_class String,
--     is_error     UInt8,
--     description  String
-- )
-- ENGINE = MergeTree
-- ORDER BY status_code;


-- CREATE TABLE IF NOT EXISTS analytics.dim_date
-- (
--     date         Date,
--     year         UInt16,
--     month        UInt8,
--     day          UInt8,
--     week         UInt8,
--     day_of_week  UInt8,
--     is_weekend   UInt8
-- )
-- ENGINE = MergeTree
-- ORDER BY date;


-- CREATE TABLE IF NOT EXISTS analytics.dim_time
-- (
--     hour        UInt8,     -- 0~23
--     minute      UInt8,     -- 0~59
--     second      UInt8,     -- 0~59
--     time_of_day String     -- dawn/morning/afternoon/evening
-- )
-- ENGINE = MergeTree
-- ORDER BY time_key;
