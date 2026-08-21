-- Fact event table
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
ENGINE = MergeTree
PARTITION BY toDate(kafka_received_at)
ORDER BY (service, event_id)
TTL toDate(kafka_received_at) + INTERVAL 3 DAY;
