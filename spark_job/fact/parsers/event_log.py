# 파일명 : spark_job/fact/parsers/event_log.py
# 목적   : Kafka 원본 이벤트를 event_log로 변환한다.

from __future__ import annotations

from pyspark.sql import DataFrame, functions as F

from ..transforms.normalize_event import normalize_event
from ..transforms.parse_event import parse_event


def parse_event_log(kafka_df: DataFrame) -> DataFrame:
    """팩트 이벤트를 반환한다."""
    parsed = parse_event(kafka_df)
    good_df = parsed.where(F.col("json").isNotNull())
    return normalize_event(good_df)
