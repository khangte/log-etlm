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

# ✅ SSOT에서 Producer만 가져와 사용 (직접 confluent-kafka 감지/폴백 제거)
from simulator.core.kafka import Producer

def build_producer(config: dict[str, Any]):
    """
    simulator.core.kafka의 Producer를 사용해 프로듀서를 생성한다.
    - SIM_SINK=stdout: _StdoutProducer
    - SIM_SINK=kafka : confluent_kafka.Producer (설치 시)
    """
    return Producer(config)  # type: ignore[call-arg]


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

    # --- BufferError 대비: flush 후 한 번 재시도 ---
    try:
        prod.produce(topic=topic, key=kbytes, value=vbytes)  # type: ignore[attr-defined]
        prod.poll(0)  # 내부 콜백 처리용
    except BufferError:
        # 내부 큐가 가득 찬 경우 → 잠깐 비우고 한 번 더 시도
        try:
            prod.flush(1.0)  # 최대 1초 동안 큐 비우기
        except Exception:
            # StdoutProducer일 수도 있으니 조용히 무시
            pass
        # 한 번 더 시도 (여기서 또 실패하면 그대로 예외 올라가게 둠)
        prod.produce(topic=topic, key=kbytes, value=vbytes)  # type: ignore[attr-defined


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
