# 파일명 : spark_job/fact/parsers/fact_event.py
# 목적   : Kafka 원본 이벤트를 fact_event로 변환한다.

from __future__ import annotations

from pyspark.sql import DataFrame

from ..transforms.normalize_event import normalize_event
from ..transforms.parse_event import parse_event
from ..transforms.validate_event import validate_event


def parse_fact_event_with_errors(
    kafka_df: DataFrame,
    *,
    store_raw_json: bool = False,
) -> tuple[DataFrame, DataFrame]:
    """팩트 이벤트와 오류 레코드를 함께 반환한다."""
    parsed = parse_event(kafka_df)
    good_df, bad_df = validate_event(parsed)
    event_df = normalize_event(good_df, store_raw_json=store_raw_json)
    return event_df, bad_df
