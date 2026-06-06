-- Fact event table
CREATE TABLE IF NOT EXISTS analytics.event_log
(
    event_ts     DateTime64(3) CODEC(Delta, ZSTD(3)),
    kafka_ingest_ts    DateTime64(3) CODEC(Delta, ZSTD(3)),
    kafka_ts     Nullable(DateTime64(3)) CODEC(Delta, ZSTD(3)),
    spark_ts Nullable(DateTime64(3)) CODEC(Delta, ZSTD(3)),
    processed_ts DateTime64(3) CODEC(Delta, ZSTD(3)),
    stored_ts    DateTime64(3) DEFAULT now64(3) CODEC(Delta, ZSTD(3)),
    created_ts   Nullable(DateTime64(3)) CODEC(Delta, ZSTD(3)),

    service        LowCardinality(String),
    domain         LowCardinality(String),
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

    user_id     Nullable(String),
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
ENGINE = MergeTree
PARTITION BY toDate(kafka_ingest_ts)
ORDER BY (kafka_ingest_ts, service)
TTL toDate(kafka_ingest_ts) + INTERVAL 3 DAY;


-- DLQ table (parse/validation failures)
CREATE TABLE IF NOT EXISTS analytics.event_log_dlq
(
    kafka_ingest_ts     DateTime64(3),
    processed_ts  DateTime64(3),
    service       Nullable(String),
    event_id      Nullable(String),
    request_id    Nullable(String),
    source_topic  Nullable(String),
    source_partition Nullable(Int32),
    source_offset    Nullable(Int64),
    source_key       Nullable(String),
    created_ts    Nullable(DateTime64(3)),
    error_type    String,
    error_message String,
    raw_json      String
)
ENGINE = MergeTree
PARTITION BY toDate(kafka_ingest_ts)
ORDER BY (kafka_ingest_ts, error_type, source_topic)
TTL kafka_ingest_ts + INTERVAL 7 DAY
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
