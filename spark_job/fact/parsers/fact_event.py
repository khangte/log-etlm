# spark_job/fact/parsers/fact_event.py

from __future__ import annotations

import os
from pyspark.sql import DataFrame

from ..transforms.normalize_event import normalize_event
from ..transforms.parse_event import parse_event
from ..transforms.validate_event import validate_event


def parse_fact_event(kafka_df: DataFrame) -> DataFrame:
    """
    Kafka에서 읽어온 DF(key, value, topic, timestamp_ms, ...)를
    analytics.fact_event 스키마에 맞는 DF로 변환한다.
    IO(write)는 하지 않고 변환만 담당.
    """
    store_raw_json = os.getenv("SPARK_STORE_RAW_JSON", "false").strip().lower() in (
        "1",
        "true",
        "yes",
        "y",
    )

    parsed = parse_event(kafka_df)
    good_df, _bad_df = validate_event(parsed)
    return normalize_event(good_df, store_raw_json=store_raw_json)
