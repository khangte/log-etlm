# -----------------------------------------------------------------------------
# 파일명 : consumers/verifier.py
# 목적   : Kafka 토픽에서 메시지를 읽어 샘플 출력/통계/스키마 검증을 수행하는 미니 컨슈머
# 사용   :
#   python consumers/verifier.py --topics logs.auth,logs.order --from-beginning
#   python consumers/verifier.py --topics logs.auth,logs.order,logs.payment,logs.notify --errors
# 옵션   :
#   --group            : 컨슈머 그룹명 (기본: log-forge-verifier)
#   --topics           : 콤마로 구분한 토픽 목록 (기본: logs.auth,...,logs.notify)
#   --errors           : 에러 전용 토픽(logs.error)도 함께 구독
#   --from-beginning   : earliest 오프셋부터 읽기
#   --stats-interval   : 통계 출력 주기(초), 기본 5
#   --sample-every     : N개마다 샘플 1건 출력, 기본 200
#   --max              : 최대 처리 건수(0이면 무한)
#
# 비고   :
# - confluent-kafka가 없으면 실행 시 친절히 안내하고 종료합니다.
# - 스키마 검증은 simulator/schema.py를 사용(필요 시 normalize로 보정 가능)
# -----------------------------------------------------------------------------

from __future__ import annotations
import argparse
import json
import sys
import time
from typing import List

# Kafka 라이브러리 확인
try:
    from confluent_kafka import Consumer  # type: ignore
    _HAS_CK = True
except Exception:
    _HAS_CK = False

# 프로젝트 내부 모듈
from simulator.core.kafka import BROKERS
from simulator.core.kafka import get_topics as _get_topics
from simulator import schema


def _default_topics() -> List[str]:
    t = _get_topics()
    return [t["auth"], t["order"], t["payment"], t["notify"]]


def build_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Log Forge: simple Kafka verifier consumer")
    p.add_argument("--group", default="log-forge-verifier", help="consumer group id")
    p.add_argument("--topics", default=",".join(_default_topics()),
                   help="comma-separated list of topics")
    p.add_argument("--errors", action="store_true", help="also subscribe logs.error")
    p.add_argument("--from-beginning", action="store_true", dest="earliest",
                   help="start from earliest offsets")
    p.add_argument("--stats-interval", type=int, default=5, help="stats print interval seconds")
    p.add_argument("--sample-every", type=int, default=200, help="print one sample every N messages")
    p.add_argument("--max", type=int, default=0, help="max messages to consume (0 = unlimited)")
    return p.parse_args()


def main() -> None:
    if not _HAS_CK:
        print("[verifier] confluent-kafka가 설치되어 있지 않습니다.", file=sys.stderr)
        print("          pip install -r simulator/requirements.txt 후 다시 시도하세요.", file=sys.stderr)
        sys.exit(2)

    ns = build_args()
    topics = [t.strip() for t in ns.topics.split(",") if t.strip()]
    if ns.errors:
        topics.append(_get_topics()["error"])

    conf = {
        "bootstrap.servers": BROKERS,
        "group.id": ns.group,
        "auto.offset.reset": "earliest" if ns.earliest else "latest",
        "enable.auto.commit": True,
    }
    c = Consumer(conf)
    c.subscribe(topics)

    total = 0
    bad = 0
    last_stats = time.time()
    sample_every = max(1, ns.sample_every)
    max_count = int(ns.max)

    print(f"[verifier] brokers={BROKERS} group={ns.group} topics={topics} reset={conf['auto.offset.reset']}")

    try:
        while True:
            msg = c.poll(1.0)  # 1초 대기
            now = time.time()

            if msg is None:
                # 주기적으로 통계 출력
                if now - last_stats >= ns.stats_interval:
                    print(f"[stats] total={total} bad={bad}")
                    last_stats = now
                continue

            if msg.error():
                # 브로커/파티션 오류 등
                print(f"[warn] kafka error: {msg.error()}", file=sys.stderr)
                continue

            try:
                payload = msg.value().decode("utf-8")
                ev = json.loads(payload)
            except Exception as e:
                bad += 1
                if (total + 1) % sample_every == 0:
                    print(f"[bad-json] topic={msg.topic()} err={e} value_len={len(msg.value() or b'')}", file=sys.stderr)
                total += 1
                continue

            # 스키마 검증
            ok, errs = schema.validate(ev)
            if not ok:
                bad += 1
                # 필요 시 보정: ev = schema.normalize(ev)
                if (total + 1) % sample_every == 0:
                    print(f"[bad-schema] topic={msg.topic()} errs={errs} ev={ev}", file=sys.stderr)

            # 샘플 출력(너무 시끄럽지 않게 N개마다)
            if (total + 1) % sample_every == 0:
                print(f"[sample] topic={msg.topic()} ev={ev}")

            total += 1

            # 최대 처리 건수 도달 시 종료
            if max_count and total >= max_count:
                break

            # 통계 주기
            if now - last_stats >= ns.stats_interval:
                print(f"[stats] total={total} bad={bad}")
                last_stats = now

    except KeyboardInterrupt:
        print("\n[verifier] interrupted by user")
    finally:
        c.close()
        print(f"[verifier] done. total={total} bad={bad}")


if __name__ == "__main__":
    main()
