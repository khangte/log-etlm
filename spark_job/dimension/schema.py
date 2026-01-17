from pyspark.sql import types as T

# ClickHouse DB 이름
CLICKHOUSE_DB: str = "analytics"

# -----------------------------------------------------------------------------
# 4) Dimension 테이블 컬럼 순서
# -----------------------------------------------------------------------------

# dim_time 스키마
DIM_TIME_SCHEMA = T.StructType(
    [
        T.StructField("time_key",    T.IntegerType(), False),  # HHMMSS 형식 (예: 93015)
        T.StructField("hour",        T.IntegerType(), False),  # 0~23
        T.StructField("minute",      T.IntegerType(), False),  # 0~59
        T.StructField("second",      T.IntegerType(), False),  # 0~59
        T.StructField("time_of_day", T.StringType(),  False),  # 새벽/아침/오후/저녁
    ]
)

# dim_date 스키마
DIM_DATE_SCHEMA = T.StructType(
    [
        T.StructField("date",        T.DateType(),    False),
        T.StructField("year",        T.IntegerType(), False),
        T.StructField("month",       T.IntegerType(), False),
        T.StructField("day",         T.IntegerType(), False),
        T.StructField("week",        T.IntegerType(), False),
        T.StructField("day_of_week", T.IntegerType(), False),
        T.StructField("is_weekend",  T.IntegerType(), False),
    ]
)

# dim_service 스키마
DIM_SERVICE_SCHEMA = T.StructType(
    [
        T.StructField("service",       T.StringType(), False),
        T.StructField("service_group", T.StringType(), False),
        T.StructField("is_active",     T.IntegerType(), False),
        T.StructField("description",   T.StringType(), True),
    ]
)

# dim_status_code 스키마
DIM_STATUS_CODE_SCHEMA = T.StructType(
    [
        T.StructField("status_code",  T.IntegerType(), False),
        T.StructField("status_class", T.StringType(),  False),
        T.StructField("is_error",     T.IntegerType(), False),
        T.StructField("description",  T.StringType(),  True),
    ]
)

# dim_user 스키마
DIM_USER_SCHEMA = T.StructType(
    [
        T.StructField("user_id",     T.StringType(), False),
        T.StructField("is_active",   T.IntegerType(), False),
        T.StructField("description", T.StringType(), True),
    ]
)

# dim_date 컬럼 순서
DIM_DATE_COLUMNS: list[str] = [
    "date",
    "year",
    "month",
    "day",
    "week",
    "day_of_week",
    "is_weekend",
]

# dim_time 컬럼 순서
DIM_TIME_COLUMNS: list[str] = [
    "time_key",
    "hour",
    "minute",
    "second",
    "time_of_day",
]

# dim_service 컬럼 순서
DIM_SERVICE_COLUMNS: list[str] = [
    "service",
    "service_group",
    "is_active",
    "description",
]

# dim_status_code 컬럼 순서
DIM_STATUS_CODE_COLUMNS: list[str] = [
    "status_code",
    "status_class",
    "is_error",
    "description",
]

# dim_user 컬럼 순서
DIM_USER_COLUMNS: list[str] = [
    "user_id",
    "is_active",
    "description",
]
