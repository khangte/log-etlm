-- Fact event table
-- ReplacingMergeTree(spark_processed_at): event_id 기준 중복 제거, 최신 spark_processed_at 보존
-- ORDER BY (service, event_id): 파티션 내 서비스별 클러스터링 + event_id dedup 키
-- 쿼리 시 FINAL 키워드로 머지 전 즉시 dedup 결과 조회 가능
DROP TABLE IF EXISTS analytics.event_log;
CREATE TABLE analytics.event_log
(
    event_timestamp      Nullable(DateTime64(3, 'UTC')) CODEC(Delta, ZSTD(3)),
    kafka_received_at    DateTime64(3, 'UTC') CODEC(Delta, ZSTD(3)),
    spark_received_at    Nullable(DateTime64(3, 'UTC')) CODEC(Delta, ZSTD(3)),
    spark_processed_at   DateTime64(3, 'UTC') CODEC(Delta, ZSTD(3)),
    clickhouse_stored_at DateTime64(3, 'UTC') DEFAULT now64(3) CODEC(Delta, ZSTD(3)),

    service        LowCardinality(String),
    api_group      LowCardinality(String),
    event_name     LowCardinality(String),
    result         LowCardinality(String),
    request_id     String,
    event_id       String,
    method         LowCardinality(String),
    route_template LowCardinality(String),
    status_code    Int32,
    duration_ms    Nullable(UInt32),

    user_id     Nullable(UInt32),
    order_id    Nullable(String),
    payment_id  Nullable(String),
    reason_code Nullable(String),
    amount      Nullable(Int32),

    topic           LowCardinality(String),
    kafka_partition Int32,
    kafka_offset    Int64
)
ENGINE = ReplacingMergeTree(spark_processed_at)
PARTITION BY toDate(kafka_received_at)
ORDER BY (service, event_id)
TTL toDate(kafka_received_at) + INTERVAL 3 DAY;
