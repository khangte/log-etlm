CREATE DATABASE IF NOT EXISTS analytics;

CREATE TABLE IF NOT EXISTS analytics.fact_log
(
    -- 시간
    event_ts   DateTime64(3)           ,  -- 실제 로그 시각 (애플리케이션에서 찍힌 timestamp)
    ingest_ts  DateTime64(3)           ,  -- Spark → ClickHouse 적재 시각

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
    user_id           String,
    notification_type String,
    product_id        Int32,
    amount            Int32,

    -- 인프라/추적용
    topic     String,       -- Kafka topic 이름

    -- 원본 JSON 백업
    raw_json  String
)
ENGINE = MergeTree
PARTITION BY toDate(event_ts)
ORDER BY (service, event_ts, request_id);


CREATE TABLE IF NOT EXISTS analytics.dim_service
(
    service       String,
    service_group String,
    is_active     UInt8
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
    hour        UInt8,     -- 0~23
    minute      UInt8,     -- 0~59
    second      UInt8,     -- 0~59
    time_of_day String     -- dawn/morning/afternoon/evening
)
ENGINE = MergeTree
ORDER BY time_key;
