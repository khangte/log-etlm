-- Dimension tables
CREATE TABLE IF NOT EXISTS analytics.dim_service
(
    service       String,
    service_group String,
    is_active     UInt8,
    description   Nullable(String)
)
ENGINE = MergeTree
ORDER BY service;

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
    time_key    UInt32,
    hour        UInt8,
    minute      UInt8,
    second      UInt8,
    time_of_day String
)
ENGINE = MergeTree
ORDER BY time_key;

CREATE TABLE IF NOT EXISTS analytics.dim_user
(
    user_id     String,
    is_active   UInt8,
    description Nullable(String)
)
ENGINE = MergeTree
ORDER BY user_id;
