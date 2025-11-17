from __future__ import annotations
from typing import Dict, Any, Optional
import os, sys, json
# from datetime import datetime, timezone # _ts 함수 제거로 불필요

# ===== 브로커/클라이언트/토픽 =====
# 컨테이너 간 통신 기본값은 kafka:9092 (호스트에서 쓸 땐 localhost:29092 등으로 교체)
BROKERS: str = "kafka:9092"
CLIENT_ID: str = "log-forge-sim"   # 프로젝트명에 맞춤

# 서비스별 토픽 맵 + error/DLQ (DLQ는 선택)
TOPICS: Dict[str, str] = {
    "auth":    "logs.auth",
    "order":   "logs.order",
    "payment": "logs.payment",
    "notify":  "logs.notify",
    "error":   "logs.error",   # 에러 복제 발행용
    "dlq":     "dlq.logs",     # 파싱 실패 등 사후 처리용(선택)
}

# (선택) 보안 설정. 기본은 None(로컬)
SECURITY: Dict[str, Any] | None = None
# 예시)
# SECURITY = {
#     "security.protocol": "SASL_SSL",
#     "sasl.mechanism": "PLAIN",
#     "sasl.username": "user",
#     "sasl.password": "pass",
# }

# ===== 전송 스위치 =====
SINK: str = os.environ.get("SIM_SINK", "kafka").lower()  # stdout | kafka

# _ts 함수 제거 (envelope 생성 로직 제거로 불필요)

# ----- stdout 전송용 더미 Producer (confluent_kafka.Producer와 인터페이스 유사) -----
class _StdoutProducer:
    """
    confluent_kafka.Producer 와 동일한 .produce(), .flush(), .close() 시그니처 제공.
    value가 dict면 JSON dump하여 1라인으로 stdout에 기록.
    """
    def __init__(self, *_, **__):
        print("[producer] Using StdoutProducer (SIM_SINK=stdout).", file=sys.stderr)

    def produce(
        self,
        topic: str, # 실제 사용되지는 않지만 인터페이스 유지를 위해 남겨둠
        value: Optional[bytes | str | dict] = None,
        key: Optional[bytes | str] = None, # 실제 사용되지는 않지만 인터페이스 유지를 위해 남겨둠
        on_delivery=None,
        **kwargs: Any,
    ) -> None:
        # value를 JSON 문자열로 변환
        if isinstance(value, dict):
            line = json.dumps(value, ensure_ascii=False, separators=(",", ":"))
        elif isinstance(value, bytes):
            line = value.decode("utf-8", errors="replace")
        elif value is None:
            line = ""
        else:
            line = str(value)

        sys.stdout.write(line + "\n")
        sys.stdout.flush()

        # on_delivery 콜백이 넘어오면 성공 콜백 호출 모사
        if callable(on_delivery):
            try:
                # topic 인자를 사용하지 않으므로, 임의의 topic을 전달
                on_delivery(None, type("Msg", (), {"topic": lambda: topic})())  # err, msg
            except Exception:
                pass

    def flush(self, *_: Any, **__: Any) -> None:
        sys.stdout.flush()

    def close(self) -> None:
        try:
            sys.stdout.flush()
        except Exception:
            pass


def get_topics() -> Dict[str, str]:
    """
    Kafka 토픽 맵을 반환한다.

    Returns:
        Dict[str, str]: {"auth":"logs.auth", ... "error":"logs.error", "dlq":"dlq.logs"}
    """
    return TOPICS.copy()


def get_producer_config() -> Dict[str, Any]:
    """
    Kafka Producer 생성에 필요한 기본 설정을 반환한다.
    - 멱등성/압축/배치/acks 등 안정성/성능 옵션 포함.

    Returns:
        Dict[str, Any]: confluent-kafka/python-kafka 등에서 사용할 설정 맵
    """
    base = {
        "bootstrap.servers": BROKERS,
        "client.id": CLIENT_ID,
        # 안정성/성능 옵션(라이브러리에 따라 키가 다를 수 있음)
        "enable.idempotence": True,
        "compression.type": "zstd",
        "acks": "all",
        # 배치/지연
        "linger.ms": 5,
        "batch.num.messages": 10000,
    }
    if SECURITY:
        base.update(SECURITY)
    return base

# ----- Producer 심볼을 이 모듈에서 직접 제공 -----
# 다른 코드가 `from simulator.core.kafka import Producer` 로 가져다 쓰더라도 동작하게 함.
if SINK == "kafka":
    try:
        from confluent_kafka import Producer as _KafkaProducer  # lazy import
        Producer = _KafkaProducer
    except Exception as e:
        # 라이브러리 미설치/오류 시 안전하게 stdout로 폴백
        Producer = _StdoutProducer  # type: ignore[assignment]
else:
    Producer = _StdoutProducer  # type: ignore[assignment]
