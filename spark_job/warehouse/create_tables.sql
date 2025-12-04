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
