from __future__ import annotations

import os
import time

from pyspark.storagelevel import StorageLevel

from ...dlq.router import handle_dlq_records
from ...dlq.transforms.build_dlq import build_dlq_df
from ...common.checkpoints import maybe_reset_checkpoint
from ...spark import build_streaming_spark
from ..readers.fact_kafka import read_fact_stream
from ..transforms.normalize_event import normalize_event
from ..transforms.parse_event import parse_event
from ..transforms.validate_event import validate_event
from ..writers.fact_writer import FACT_EVENT_CHECKPOINT_DIR, write_fact_event_batch


def start_fact_event_stream(spark=None):
    """정상 이벤트 스트림을 읽어 fact_event에 적재한다."""
    if spark is None:
        spark = build_streaming_spark(master=os.getenv("SPARK_MASTER_URL"))

    maybe_reset_checkpoint(FACT_EVENT_CHECKPOINT_DIR)

    kafka_df = read_fact_stream(spark)

    store_raw_json = os.getenv("SPARK_STORE_RAW_JSON", "false").strip().lower() in (
        "1",
        "true",
        "yes",
        "y",
    )
    dlq_enabled = os.getenv("SPARK_DLQ_ENABLED", "true").strip().lower() in (
        "1",
        "true",
        "yes",
        "y",
    )
    def _write_fact_batch(batch_df, batch_id: int) -> None:
        start_time = time.perf_counter()
        parsed = parse_event(batch_df)
        cached = False
        if dlq_enabled:
            parsed = parsed.persist(StorageLevel.MEMORY_AND_DISK)
            cached = True

        try:
            parse_done = time.perf_counter()
            good_df, bad_df = validate_event(parsed)
            validate_done = time.perf_counter()
            event_df = normalize_event(good_df, store_raw_json=store_raw_json)
            normalize_done = time.perf_counter()
            write_fact_event_batch(event_df, batch_id=batch_id)
            write_done = time.perf_counter()

            dlq_done = write_done
            if dlq_enabled:
                parse_error_dlq_df = build_dlq_df(bad_df)
                handle_dlq_records(parse_error_dlq_df, batch_id=batch_id)
                dlq_done = time.perf_counter()

            total_done = dlq_done
            print(
                "[batch timing] "
                f"batch_id={batch_id} "
                f"parse={parse_done - start_time:.3f}s "
                f"validate={validate_done - parse_done:.3f}s "
                f"normalize={normalize_done - validate_done:.3f}s "
                f"write={write_done - normalize_done:.3f}s "
                f"dlq={dlq_done - write_done:.3f}s "
                f"total={total_done - start_time:.3f}s"
            )
        finally:
            if cached:
                parsed.unpersist()

    return (
        kafka_df.writeStream.outputMode("append")
        .foreachBatch(_write_fact_batch)
        .option("checkpointLocation", FACT_EVENT_CHECKPOINT_DIR)
        .queryName("fact_event_stream")
        .start()
    )
