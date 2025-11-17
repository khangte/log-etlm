# -----------------------------------------------------------------------------
# 파일명 : simulator/core/runner.py
# 목적   : 시뮬레이터 메인 루프(RPS/시간대 가중치/버스트/믹스 분배 → 생성 → 발행)
# 사용   : main.py에서 load_profile()/load_routes() 후 run(profile, routes, ...)
# 설명   :
#   - 최소 스키마(9키) 기준으로 services/*가 이벤트를 생성
#   - 여기서는 RPS 결정/분배와 Kafka 발행만 담당
#   - 에러(lvl="E")는 서비스 토픽 + error 토픽으로 '복제 발행'(옵션) 처리
# -----------------------------------------------------------------------------

from __future__ import annotations
from typing import Dict, Any, List, Tuple
import copy # copy 모듈 추가
import json # json 모듈 추가
import math
import time

from simulator.core.kafka import get_topics, get_producer_config
from simulator.core.timeband import load_bands, current_hour_kst, pick_multiplier
from simulator.core.stats import Stats
from simulator.core.signalz import GracefulKiller
from simulator import generator
from simulator import producer  # producer.produce(), producer.build_producer(), producer.flush_safely()
from simulator import schema  # (최소 9-키 스키마 정의)


def _apply_bursts(base_rps: int, bursts: List[Dict[str, Any]], elapsed_sec: int) -> int:
    """
    버스트 정의가 있으면 '해당 시점 이후' rps를 덮어쓴다(교체 모델).
    필요한 경우 합성 모델로 바꿀 수 있다.

    Args:
        base_rps: time_weights까지 반영된 rps
        bursts: [{at_sec: int, rps: int}, ...]
        elapsed_sec: 시작 후 경과 초

    Returns:
        int: 최종 rps
    """
    effective = base_rps
    # at_sec 오름차순 적용
    for b in sorted(bursts or [], key=lambda x: x.get("at_sec", 0)):
        if elapsed_sec >= int(b.get("at_sec", 0)):
            effective = int(b.get("rps", effective))
    return max(0, effective)


def _split_counts(total: int, mix: Dict[str, float]) -> Dict[str, int]:
    """
    total 건수를 서비스 비율(mix)에 따라 정수로 분배.
    - 소수점은 라운딩되므로 합계 오차가 발생할 수 있어, remainder를 임의 서비스에 분배한다.

    Args:
        total: 이번 1초에 생성할 전체 건수
        mix: {"auth":0.2, "order":0.4, ...}

    Returns:
        Dict[str,int]: 서비스별 배정 건수
    """
    keys = list(mix.keys())
    raw = {k: total * float(mix[k]) for k in keys}
    alloc = {k: int(math.floor(v)) for k, v in raw.items()}
    # 남은 나머지 분배
    remain = total - sum(alloc.values())
    for k in sorted(keys, key=lambda x: raw[x]-alloc[x], reverse=True):
        if remain <= 0:
            break
        alloc[k] += 1
        remain -= 1
    return alloc


def run(
    profile: Dict[str, Any],
    routes_map: Dict[str, List[Dict[str, Any]]],
    *,
    replicate_error_to_topic: bool = True,
    dry_run: bool = False,
    log_interval_sec: int = 5,
) -> None:
    """
    시뮬레이터 메인 루프 실행.

    Args:
        profile: profiles/*.yaml 로드 결과
        routes_map: templates/routes.yml 로드 결과(서비스별 라우트 리스트)
        replicate_error_to_topic: 에러(lvl="E")일 때 error 토픽에 복제 발행할지
        dry_run: True면 Kafka 발행 대신 stdout만(미니멀 출력)
        log_interval_sec: N초마다 통계 로그 출력

    동작:
        1) base_rps ← profile.rps
        2) multiplier ← time_weights(mode) by KST hour
        3) rps_now ← base_rps * multiplier
        4) bursts 있으면 rps_now 교체
        5) mix 비율로 svc별 건수 분배
        6) generator.generate_batch() → producer.produce()
    """
    duration_sec = int(profile.get("duration_sec", 60))
    base_rps = int(profile.get("rps", 100))
    mix = profile.get("mix", {"auth": 0.25, "order": 0.25, "payment": 0.25, "notify": 0.25})

    # 시간대 가중치 준비
    bands = load_bands(profile.get("time_weights", []))
    mode = profile.get("weight_mode", "uniform")

    # 프로듀서 준비
    topics = get_topics()
    stats = Stats()
    killer = GracefulKiller()
    prod = None
    if not dry_run:
        prod = producer.build_producer(get_producer_config())

    # 루프
    started = time.time()
    next_log = started + log_interval_sec
    elapsed = 0

    while elapsed < duration_sec and not killer.should_stop():
        # 1) multiplier 선택 (KST 시각 기준)
        hour_kst = current_hour_kst()
        mult = pick_multiplier(bands, hour_kst, mode) if bands else 1.0

        # 2) 이번 초의 rps
        rps_now = int(max(0, round(base_rps * mult)))

        # 3) bursts 적용(교체 모델)
        rps_now = _apply_bursts(rps_now, profile.get("bursts", []), elapsed)

        if rps_now <= 0:
            time.sleep(1.0)
            elapsed = int(time.time() - started)
            if time.time() >= next_log:
                print(stats.summary())
                next_log = time.time() + log_interval_sec
            continue

        # 4) mix 분배
        alloc = _split_counts(rps_now, mix)

        # 5) 서비스별 배치 생성 및 발행
        for svc, cnt in alloc.items():
            if cnt <= 0:
                continue
            routes = routes_map.get(svc, [])
            batch = generator.generate_batch(service=svc, count=cnt, routes=routes, profile=profile)

            if dry_run:
                # 너무 시끄러우니 샘플 몇 개만 출력
                print(f"[dry-run] {svc} n={len(batch)} example={batch[0] if batch else None}")
            else:
                for ev in batch:
                    ok, errs = schema.validate(ev)
                    if not ok:
                        # 보정 시도
                        ev = schema.normalize(ev)
                        # 그래도 중요한 프로젝트면 errs를 로그/에러토픽/DLQ로 보냄
                        # producer.produce(prod, topic=topics["dlq"], key=..., value=...errs...)

                    key = ev.get("rid", "")
                    val = json.dumps(ev, ensure_ascii=False) # producer가 직접 직렬화
                    producer.produce(prod, topic=topics.get(svc, f"logs.{svc}"), key=key, value=val)

                    # 에러 복제 발행 옵션
                    if replicate_error_to_topic and ev.get("lvl") == "E":
                        # 에러 복제본 생성: svc 필드를 "error"로 교체
                        err_ev = copy.deepcopy(ev)
                        err_ev["svc"] = "error"
                        val_err = json.dumps(err_ev, ensure_ascii=False)
                        producer.produce(prod, topic=topics["error"], key=key, value=val_err)

            stats.add_sent(svc, len(batch))

        # 6) 통계 로그
        if time.time() >= next_log:
            print(stats.summary())
            next_log = time.time() + log_interval_sec

        # 7) 1초 페이스 유지
        time.sleep(1.0)
        elapsed = int(time.time() - started)

    # 종료 처리
    if prod is not None:
        producer.flush_safely(prod)
    print("[runner] finished. " + stats.summary())
