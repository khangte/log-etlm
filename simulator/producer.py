# -----------------------------------------------------------------------------
# 파일명 : simulator/producer.py
# 목적   : Kafka 발행 어댑터 (confluent-kafka가 있으면 사용, 없으면 안전 폴백)
# 사용   :
#   p = build_producer(config)            # 프로듀서 생성
#   produce(p, topic, key, value)         # 1건 발행
#   flush_safely(p)                       # 종료 전 플러시
#
# 주의   : value는 JSON 문자열(str)로 넘기는 것을 권장(내부에서 bytes로 변환)
# -----------------------------------------------------------------------------

from __future__ import annotations
from typing import Any, Optional
import sys

# 가용하면 confluent-kafka 사용, 아니면 폴백
try:
    from confluent_kafka import Producer as _CKProducer  # type: ignore
    _USE_CK = True
except Exception:
    _CKProducer = None
    _USE_CK = False


class _DummyProducer:
    """
    Kafka 라이브러리가 없을 때를 위한 아주 간단한 폴백 구현.
    - produce() 호출 시 표준출력에 한 줄 로그만 남긴다.
    - flush()는 no-op.
    """
    def __init__(self, *_args, **_kwargs) -> None:
        print("[producer] confluent-kafka not found → using DummyProducer (stdout only).", file=sys.stderr)

    def produce(self, topic: str, key: Optional[bytes], value: bytes, on_delivery=None) -> None:
        # 너무 시끄럽지 않게, 토픽/키 길이/바이트 수만 출력
        klen = len(key) if key else 0
        print(f"[produce:dummy] topic={topic} key_len={klen} value_len={len(value)}")

    def flush(self, timeout: float | None = None) -> None:
        return None


def build_producer(config: dict[str, Any]):
    """
    Kafka 프로듀서를 생성한다.
    - confluent-kafka가 설치되어 있으면 실제 Producer를,
      없으면 DummyProducer를 반환한다.

    Args:
        config: core.kafka.get_producer_config() 결과

    Returns:
        object: confluent_kafka.Producer 또는 _DummyProducer
    """
    if _USE_CK:
        return _CKProducer(config)
    return _DummyProducer()


def produce(prod, *, topic: str, key: str | bytes | None, value: str | bytes) -> None:
    """
    단일 메시지를 발행한다.

    Args:
        prod: build_producer()가 반환한 객체
        topic: 카프카 토픽명
        key: 파티셔닝 키(보통 request id). None 가능
        value: 메시지 본문(JSON 문자열 권장)
    """
    kbytes: Optional[bytes]
    if key is None:
        kbytes = None
    elif isinstance(key, bytes):
        kbytes = key
    else:
        kbytes = key.encode("utf-8")

    if isinstance(value, str):
        vbytes = value.encode("utf-8")
    else:
        vbytes = value

    # confluent-kafka의 경우 콜백을 넣을 수 있지만, 여기선 생략(단순화)
    prod.produce(topic=topic, key=kbytes, value=vbytes)  # type: ignore[attr-defined]


def flush_safely(prod) -> None:
    """
    프로듀서의 내부 큐를 비운다. (DummyProducer면 no-op)

    Args:
        prod: build_producer()가 반환한 객체
    """
    try:
        prod.flush(5.0)  # 최대 5초 대기
    except Exception:
        pass
