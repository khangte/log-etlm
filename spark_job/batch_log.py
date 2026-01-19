from __future__ import annotations

import os
import time

from .clickhouse.settings import get_batch_timing_log_settings


def append_batch_log(line: str) -> None:
    """배치 타이밍 로그를 파일에 추가한다."""
    log_path = get_batch_timing_log_settings().log_path
    if not log_path:
        return
    try:
        log_dir = os.path.dirname(log_path)
        if log_dir:
            os.makedirs(log_dir, exist_ok=True)
        utc_ts = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        with open(log_path, "a", encoding="utf-8") as logfile:
            logfile.write(f"{utc_ts} {line}\n")
    except Exception as exc:
        print(f"[spark batch] log write failed: {exc}")
