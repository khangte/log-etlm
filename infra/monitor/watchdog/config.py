from __future__ import annotations

import os
import re
from pathlib import Path


ENV_PATH = Path(__file__).parents[1] / "watchdog.env"
if ENV_PATH.exists():
    for _line in ENV_PATH.read_text().splitlines():
        _line = _line.strip()
        if not _line or _line.startswith("#") or "=" not in _line:
            continue
        _key, _value = _line.split("=", 1)
        _key = _key.strip()
        _value = _value.strip().strip('"').strip("'")
        if _key and _key not in os.environ:
            os.environ[_key] = _value

TARGET_CONTAINERS: list[str] = ["kafka", "spark-driver", "clickhouse", "grafana"]

LOG_PATTERNS: dict[str, list[re.Pattern[str]]] = {
    "kafka": [
        re.compile(r"OutOfMemoryError", re.IGNORECASE),
        re.compile(r"Fatal error", re.IGNORECASE),
    ],
    "spark-driver": [
        re.compile(r"OutOfMemoryError"),
        re.compile(r"StreamingQueryException"),
        re.compile(r"Job aborted"),
        re.compile(r"Code:\s*241"),
        re.compile(r"MEMORY_LIMIT_EXCEEDED", re.IGNORECASE),
    ],
    "clickhouse": [
        re.compile(r"Code:\s*241"),
        re.compile(r"Memory limit exceeded", re.IGNORECASE),
        re.compile(r"DB::Exception"),
    ],
    "grafana": [
        re.compile(r"level=error", re.IGNORECASE),
        re.compile(r"panic:", re.IGNORECASE),
    ],
}

ALERT_WEBHOOK_URL = os.getenv("ALERT_WEBHOOK_URL")
HEALTH_INTERVAL_SEC = int(os.getenv("HEALTH_INTERVAL_SEC", "30"))
ALERT_COOLDOWN_SEC = int(os.getenv("ALERT_COOLDOWN_SEC", "300"))
ALERT_BREACH_GRACE_SEC = int(os.getenv("ALERT_BREACH_GRACE_SEC", "300"))

CH_MONITOR_ENABLED = os.getenv("CH_MONITOR_ENABLED", "true").lower() == "true"
CH_HTTP_URL = os.getenv("CH_HTTP_URL", "http://localhost:8123")
CH_DB = os.getenv("CH_DB", "analytics")
CH_USER = os.getenv("CH_USER", os.getenv("CLICKHOUSE_USER", "log_user"))
CH_PASSWORD = os.getenv("CH_PASSWORD", os.getenv("CLICKHOUSE_PASSWORD", ""))
CH_TIMEOUT_SEC = int(os.getenv("CH_TIMEOUT_SEC", "5"))
CH_QUERY_INTERVAL_SEC = int(os.getenv("CH_QUERY_INTERVAL_SEC", "600"))

P95_PRODUCER_TO_KAFKA_MS_MAX = int(
    os.getenv("P95_PRODUCER_TO_KAFKA_MS_MAX", os.getenv("P95_QUEUE_MS_MAX", "60000"))
)
P95_KAFKA_TO_SPARK_INGEST_MS_MAX = int(
    os.getenv(
        "P95_KAFKA_TO_SPARK_INGEST_MS_MAX", os.getenv("P95_PUBLISH_MS_MAX", "60000")
    )
)
P95_SPARK_PROCESSING_MS_MAX = int(os.getenv("P95_SPARK_PROCESSING_MS_MAX", "60000"))
P95_SPARK_TO_STORED_MS_MAX = int(
    os.getenv("P95_SPARK_TO_STORED_MS_MAX", os.getenv("P95_SINK_MS_MAX", "60000"))
)
P95_E2E_MS_MAX = int(os.getenv("P95_E2E_MS_MAX", "60000"))
FRESHNESS_MS_MAX = int(os.getenv("FRESHNESS_MS_MAX", "120000"))
EPS_MIN = float(os.getenv("EPS_MIN", "1"))
ERROR_RATE_PCT_MAX = float(os.getenv("ERROR_RATE_PCT_MAX", "1"))
DLQ_RATE_PCT_MAX = float(os.getenv("DLQ_RATE_PCT_MAX", "1"))
