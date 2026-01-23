from pyspark.sql import types as T

# -----------------------------------------------------------------------------
# 1) Kafka value(JSON) 스키마
#    - simulator v2 이벤트 필드 + 레거시 필드 동시 수용
#    - from_json(col("value"), log_value_schema) 에서 사용
# -----------------------------------------------------------------------------

# Kafka 이벤트 value 스키마
log_value_schema: T.StructType = T.StructType(
    [
        # v2 이벤트 필드(현재 simulator 기준)
        T.StructField("event_id",       T.StringType(),  True),
        T.StructField("event_name",     T.StringType(),  True),
        T.StructField("domain",         T.StringType(),  True),
        T.StructField("ts_ms",          T.LongType(),    True),
        T.StructField("created_ts_ms",  T.LongType(),    True), # E2E 측정의 기준이 되는 이벤트 생성 시각
        T.StructField("service",        T.StringType(),  True),
        T.StructField("request_id",     T.StringType(),  True),
        T.StructField("method",         T.StringType(),  True),
        T.StructField("route_template", T.StringType(),  True),
        T.StructField("status_code",    T.IntegerType(), True),
        T.StructField("duration_ms",    T.IntegerType(), True),
        T.StructField("result",         T.StringType(),  True),
        T.StructField("user_id",        T.StringType(),  True),
        T.StructField("order_id",       T.StringType(),  True),
        T.StructField("payment_id",     T.StringType(),  True),
        T.StructField("reason_code",    T.StringType(),  True),
        T.StructField("api_group",      T.StringType(),  True),
        T.StructField("amount",         T.IntegerType(), True),

        # 레거시 호환 필드(기존 이벤트 포맷)
        T.StructField("timestamp_ms",      T.LongType(),    True),
        T.StructField("level",             T.StringType(),  True),
        T.StructField("path",              T.StringType(),  True),
        T.StructField("event",             T.StringType(),  True),
        T.StructField("product_id",        T.IntegerType(), True),
    ]
)


# -----------------------------------------------------------------------------
# 2) ClickHouse analytics.fact_event 적재 컬럼 순서
#    - stored_ts는 ClickHouse DEFAULT(now64)로 채워지므로 제외
# -----------------------------------------------------------------------------

# fact_event 컬럼 순서
FACT_EVENT_COLUMNS: list[str] = [
    # 1) 시간
    "event_ts",
    "ingest_ts",
    "kafka_ts",
    "spark_ingest_ts",
    "processed_ts",
    "created_ts",
    "ingest_ms",
    "process_ms",

    # 2) 식별자 / 상관관계
    "request_id",
    "event_id",

    # 3) 분류 메타
    "service",
    "domain",
    "api_group",
    "event_name",
    "result",

    # 4) HTTP/요청 정보
    "method",
    "route_template",
    "path",
    "status_code",
    "duration_ms",

    # 5) 로그 레벨/메시지
    "level",
    "event",

    # 6) 비즈니스 필드
    "user_id",
    "order_id",
    "payment_id",
    "reason_code",
    "product_id",
    "amount",

    # 7) Kafka 메타(재처리/포렌식 핵심)
    "topic",
    "kafka_partition",
    "kafka_offset",

    # 8) 원문
    "raw_json",
]
