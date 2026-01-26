# 파일명 : spark_job/main.py
# 목적   : spark-submit 진입점

from __future__ import annotations

from datetime import datetime, timezone
import json
import os
from pyspark.sql.streaming import StreamingQueryException, StreamingQueryListener

from common.get_env import get_env_str
from spark_job.stream_ingest import start_event_ingest_streams
from spark_job.spark import build_streaming_spark


def _progress_log_path() -> str:
    """스트리밍 진행 로그 경로를 반환한다."""
    return os.getenv("SPARK_PROGRESS_LOG_PATH", "/data/log-etlm/spark-events/spark_progress.log")


class JsonProgressListener(StreamingQueryListener):
    """StreamingQueryProgress를 JSON lines로 기록한다."""

    def __init__(self, log_path: str):
        super().__init__()
        self._log_path = log_path

    def _write(self, payload: dict) -> None:
        if not self._log_path:
            return
        try:
            log_dir = os.path.dirname(self._log_path)
            if log_dir:
                os.makedirs(log_dir, exist_ok=True)
            with open(self._log_path, "a", encoding="utf-8") as logfile:
                logfile.write(json.dumps(payload, ensure_ascii=False) + "\n")
        except Exception as exc:
            print(f"[spark progress] log write failed: {exc}")

    def onQueryStarted(self, event) -> None:  # type: ignore[override]
        ts = datetime.now(timezone.utc).isoformat()
        self._write(
            {
                "ts": ts,
                "event": "started",
                "id": str(event.id),
                "runId": str(event.runId),
                "name": event.name,
            }
        )

    def onQueryProgress(self, event) -> None:  # type: ignore[override]
        ts = datetime.now(timezone.utc).isoformat()
        try:
            progress = json.loads(event.progress.json)
        except Exception:
            progress = {"raw": event.progress.json}
        self._write(
            {
                "ts": ts,
                "queryName": event.progress.name,
                "id": str(event.progress.id),
                "runId": str(event.progress.runId),
                "progress": progress,
            }
        )

    def onQueryTerminated(self, event) -> None:  # type: ignore[override]
        ts = datetime.now(timezone.utc).isoformat()
        self._write(
            {
                "ts": ts,
                "event": "terminated",
                "id": str(event.id),
                "runId": str(event.runId),
                "exception": event.exception,
            }
        )


def run_event_ingest() -> None:
    """Spark 스트리밍 적재 작업을 실행한다."""
    spark = None
    try:
        master_url = get_env_str(os.environ, "SPARK_MASTER_URL")
        spark = build_streaming_spark(master=master_url)
        spark.sparkContext.setLogLevel("INFO")

        spark.streams.addListener(JsonProgressListener(_progress_log_path()))

        start_event_ingest_streams(spark)

        try:
            spark.streams.awaitAnyTermination()
        except StreamingQueryException as exc:
            # 드라이버 종료 원인 파악을 위해 전체 예외 메시지 출력
            print(f"[❌ 스트리밍 쿼리 예외] {exc}")
            raise

    except Exception as exc:
        print(f"[❌ SparkSession] 예기치 않은 오류: {exc}")
        raise

    finally:
        if spark:
            print("[ℹ️ SparkSession] 세션 종료.")
            spark.stop()


if __name__ == "__main__":
    run_event_ingest()
