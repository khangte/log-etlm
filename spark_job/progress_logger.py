from __future__ import annotations

import json
import threading
import time
from typing import Iterable


def _append_progress_log(record: dict, log_path: str | None) -> None:
    """스트리밍 진행 정보를 JSON 한 줄로 저장한다."""
    line = json.dumps(record, ensure_ascii=True, default=str)
    if not log_path:
        print(f"[ℹ️ spark progress] {line}")
        return
    try:
        with open(log_path, "a", encoding="utf-8") as f:
            f.write(line + "\n")
    except Exception as exc:
        print(f"[⚠️ spark progress] log write failed: {exc}")


def start_progress_logger(
    queries: Iterable,
    interval_sec: float,
    log_path: str | None,
) -> tuple[threading.Event, threading.Thread]:
    """스트리밍 쿼리 진행 정보를 주기적으로 기록하는 스레드를 시작한다."""
    stop_event = threading.Event()
    last_batch_by_query: dict[str, int] = {}

    def _worker() -> None:
        while not stop_event.is_set():
            for query in queries:
                try:
                    progress = query.lastProgress
                except Exception:
                    continue
                if not progress:
                    continue
                batch_id = progress.get("batchId")
                query_key = f"{query.id}-{query.runId}"
                if batch_id is not None and last_batch_by_query.get(query_key) == batch_id:
                    continue
                last_batch_by_query[query_key] = batch_id
                _append_progress_log(
                    {
                        "ts": time.strftime("%Y-%m-%d %H:%M:%S"),
                        "queryName": query.name,
                        "id": str(query.id),
                        "runId": str(query.runId),
                        "progress": progress,
                    },
                    log_path,
                )
            stop_event.wait(interval_sec)

    thread = threading.Thread(
        target=_worker,
        name="spark-progress-logger",
        daemon=True,
    )
    thread.start()
    return stop_event, thread
