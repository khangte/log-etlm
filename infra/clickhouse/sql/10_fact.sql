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
    path           String CODEC(ZSTD(3)),
    status_code    Int32,
    duration_ms    Nullable(UInt32),
    level          LowCardinality(String),

    user_id     Nullable(UInt32),
    order_id    Nullable(String),
    payment_id  Nullable(String),
    reason_code Nullable(String),
    product_id  Nullable(Int32),
    amount      Nullable(Int32),

    topic           LowCardinality(String),
    kafka_partition Int32,
    kafka_offset    Int64,

    raw_json  String CODEC(ZSTD(3))
)
ENGINE = ReplacingMergeTree(spark_processed_at)
PARTITION BY toDate(kafka_received_at)
ORDER BY (service, event_id)
TTL toDate(kafka_received_at) + INTERVAL 3 DAY;


-- DLQ table (parse/validation failures)
CREATE TABLE IF NOT EXISTS analytics.event_log_dlq
(
    kafka_received_at    DateTime64(3, 'UTC'),
    spark_processed_at   DateTime64(3, 'UTC'),
    service       Nullable(String),
    event_id      Nullable(String),
    request_id    Nullable(String),
    source_topic  Nullable(String),
    source_partition Nullable(Int32),
    source_offset    Nullable(Int64),
    source_key       Nullable(String),
    event_timestamp  Nullable(DateTime64(3, 'UTC')),
    error_type    String,
    error_message String,
    raw_json      String
)
ENGINE = MergeTree
PARTITION BY toDate(kafka_received_at)
ORDER BY (kafka_received_at, error_type, source_topic)
TTL kafka_received_at + INTERVAL 7 DAY
SETTINGS allow_nullable_key=1;


-- Stream batch guard table (best-effort idempotency for foreachBatch sink)
CREATE TABLE IF NOT EXISTS analytics.stream_batch_guard
(
    stream_name   LowCardinality(String),
    target_table  LowCardinality(String),
    batch_id      Int64,
    committed_at  DateTime64(3) DEFAULT now64(3)
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(committed_at)
ORDER BY (stream_name, target_table, batch_id)
TTL committed_at + INTERVAL 30 DAY;
