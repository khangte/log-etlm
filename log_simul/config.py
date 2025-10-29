from __future__ import annotations

# 출력 플러그인 설정
OUTPUT_PLUGIN: str = "stdout"

# 시뮬레이터 동작 설정
SERVICES: list[str] = ["auth", "order", "payment", "notify"]

# Ingest HTTP 설정
INGEST_HTTP: str = "http://localhost:8000"
INGEST_HTTP_BATCH: str = "/logs/batch"

# HTTP 타임아웃 설정 (초)
HTTP_TIMEOUT: float = 30.0

# Kafka 설정
KAFKA_BOOTSTRAP_SERVERS: str = "localhost:9092"
KAFKA_TOPIC: str = "logs"
