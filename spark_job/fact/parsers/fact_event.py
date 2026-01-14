# spark_job/fact/fact_event.py

from __future__ import annotations

import os
from pyspark.sql import DataFrame

from ..transforms.normalize_event import normalize_event
from ..transforms.parse_event import parse_event
from ..transforms.validate_event import validate_event


def parse_fact_event_with_errors(kafka_df: DataFrame) -> tuple[DataFrame, DataFrame]:
    """
    Kafka raw DF -> fact_event DF + 파싱/검증 실패 DF를 함께 반환한다.
    """
    store_raw_json = os.getenv("SPARK_STORE_RAW_JSON", "false").strip().lower() in (
        "1",
        "true",
        "yes",
        "y",
    )

    parsed = parse_event(kafka_df)
    good_df, bad_df = validate_event(parsed)
    event_df = normalize_event(good_df, store_raw_json=store_raw_json)
    return event_df, bad_df

