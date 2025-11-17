# -----------------------------------------------------------------------------
# 파일명 : simulator/core/kafka.py
# 목적   : Kafka 관련 설정/유틸만 별도 모듈로 분리
# 사용   : runner/producer 등에서 import하여 토픽/프로듀서 설정 접근
# 주의   : .env 사용 금지. 이 파일이 Kafka 설정의 단일 진실 소스(SSOT)
# -----------------------------------------------------------------------------

from __future__ import annotations
from typing import Dict, Any, Optional
import os, sys, json
from datetime import datetime, timezone

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

def _ts() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")

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
        topic: str,
        value: Optional[bytes | str | dict] = None,
        key: Optional[bytes | str] = None,
        on_delivery=None,
        **kwargs: Any,
    ) -> None:
        # value를 문자열이면 그대로, JSON문자열이면 json.loads로 객체 변환 (value가 '객체'가 됨)

        # value 직렬화 (dict -> JSON, str -> 그대로, bytes -> UTF-8 가정)
        # if isinstance(value, dict):
        #     payload_str = json.dumps(value, ensure_ascii=False, separators=(",", ":"))
        # elif isinstance(value, bytes):
        #     payload_str = value.decode("utf-8", errors="replace")
        # elif value is None:
        #     payload_str = ""
        # else:
        #     payload_str = str(value)

        # 주의: 최소 침습을 위해 envelope 추가 없이 "원본 payload"만 출력
        # line = payload_str

        # 필요 시 토픽/키/타임스탬프를 함께 남기고 싶다면 아래 주석 해제
        # value 직렬화 준비
        if isinstance(value, dict):
            payload = value                       # dict는 그대로 담음
        elif isinstance(value, bytes):
            payload = value.decode("utf-8", errors="replace")
        elif value is None:
            payload = ""
        else:
            payload = str(value)

        try:
            payload = json.loads(payload)  # 문자열이면 객체로 변환
        except Exception:
            pass  # JSON 아니면 그대로 문자열 유지
            
        # ✅ envelope 켜기: ts/topic/key/value 를 하나의 JSON 라인으로 출력
        line = json.dumps(
            {
                "ts": _ts(),
                "topic": topic,
                "key": (key.decode() if isinstance(key, (bytes, bytearray)) else key),
                "value": payload,
            },
            ensure_ascii=False,
            separators=(",", ":"),
        )

        sys.stdout.write(line + "\n")
        sys.stdout.flush()

        # on_delivery 콜백이 넘어오면 성공 콜백 호출 모사
        if callable(on_delivery):
            try:
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
