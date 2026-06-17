#!/usr/bin/env python3
# 용도: docker-compose.yml과 config/env/*.env를 읽어
#       EPS 기반 파이프라인 각 단계의 처리량·파티션·메모리 적절성을 계산한다.
#
# 사용법:
#   python3 scripts/capacity_calc.py              # 기본 (5000, 10000 EPS)
#   python3 scripts/capacity_calc.py 3000 7000    # 직접 EPS 지정
from __future__ import annotations

import re
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml

REPO_ROOT = Path(__file__).resolve().parent.parent
COMPOSE_PATH = REPO_ROOT / "docker-compose.yml"
ENV_DIR = REPO_ROOT / "config" / "env"
PROFILES_YML = REPO_ROOT / "log_simulator" / "config" / "profiles.yml"

AVG_EVENT_BYTES = 310  # 실측 평균 (orjson 직렬화 기준)


# ---------------------------------------------------------------------------
# docker-compose.yml 파싱
# ---------------------------------------------------------------------------

def _load_compose() -> dict[str, Any]:
    with open(COMPOSE_PATH, encoding="utf-8") as f:
        return yaml.safe_load(f)


def _parse_env_list(env_list: list[str]) -> dict[str, str]:
    """docker-compose environment 리스트를 dict로 변환한다.

    '${VAR:-default}' 형태의 기본값을 추출하고, 참조만 있는 '${VAR}' 는 빈 문자열로 처리한다.
    """
    result: dict[str, str] = {}
    for item in env_list:
        if "=" not in item:
            continue
        key, raw = item.split("=", 1)
        # ${VAR:-default} → default
        m = re.search(r"\$\{[^}]+:-([^}]*)\}", raw)
        if m:
            result[key] = m.group(1).strip()
        # ${VAR} 만 있으면 빈 문자열
        elif re.search(r"\$\{[^}]+\}", raw):
            result[key] = ""
        else:
            result[key] = raw.strip()
    return result


def _parse_memory_mb(value: str) -> int:
    """'1g', '1500m', '768m' 형태를 MB 정수로 변환한다."""
    v = value.strip().lower()
    if v.endswith("g"):
        return int(float(v[:-1]) * 1024)
    if v.endswith("m"):
        return int(v[:-1])
    return int(v)


def _parse_kafka_topics(compose: dict[str, Any]) -> dict[str, int]:
    """kafka-init command에서 토픽별 파티션 수를 파싱한다."""
    cmd = compose["services"]["kafka-init"]["command"]
    script = cmd[0] if isinstance(cmd, list) else cmd
    topics: dict[str, int] = {}
    for line in script.splitlines():
        line = line.strip()
        if line.startswith("#") or "--topic" not in line:
            continue
        tm = re.search(r"--topic\s+(\S+)", line)
        pm = re.search(r"--partitions\s+(\d+)", line)
        if tm and pm:
            topics[tm.group(1)] = int(pm.group(1))
    return topics


def _parse_trigger_sec(raw: str) -> int:
    """'4 seconds', '4s' 형태를 초 정수로 변환한다."""
    raw = raw.strip().lower()
    m = re.match(r"(\d+)\s*(s|sec|secs|second|seconds)?", raw)
    return int(m.group(1)) if m else 4


# ---------------------------------------------------------------------------
# 설정 로더
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class ClusterConfig:
    """docker-compose.yml에서 읽은 클러스터 설정을 담는다."""
    worker_cores: dict[str, int]
    worker_memory_mb: dict[str, int]
    executor_memory_mb: int
    executor_overhead_mb: int
    driver_memory_mb: int
    driver_overhead_mb: int
    kafka_topics: dict[str, int]           # fact 토픽만 (logs.error 제외)
    kafka_retention_bytes: int
    kafka_retention_hours: int
    trigger_sec: int
    pre_coalesce_partitions: int
    compose_defaults: dict[str, str]       # spark-driver 환경변수 기본값

    @property
    def total_cores(self) -> int:
        return sum(self.worker_cores.values())

    @property
    def total_kafka_partitions(self) -> int:
        return sum(self.kafka_topics.values())


def load_cluster_config() -> ClusterConfig:
    """docker-compose.yml을 파싱해 ClusterConfig를 반환한다."""
    compose = _load_compose()
    services = compose["services"]

    # worker 코어/메모리
    worker_cores: dict[str, int] = {}
    worker_memory_mb: dict[str, int] = {}
    for name in ("spark-worker-1", "spark-worker-2"):
        cmd = services[name]["command"]
        cores_idx = cmd.index("--cores") + 1
        mem_idx = cmd.index("--memory") + 1
        worker_cores[name] = int(cmd[cores_idx])
        worker_memory_mb[name] = _parse_memory_mb(cmd[mem_idx])

    # spark-driver 환경변수
    driver_env = _parse_env_list(services["spark-driver"]["environment"])

    # kafka 설정
    kafka_env = services["kafka"]["environment"]
    retention_bytes = int(kafka_env.get("KAFKA_LOG_RETENTION_BYTES", 21_474_836_480))
    retention_hours = int(kafka_env.get("KAFKA_LOG_RETENTION_HOURS", 2))

    # kafka-init에서 토픽 파싱 후 fact 토픽만 추출
    all_topics = _parse_kafka_topics(compose)
    fact_topics_str = driver_env.get("SPARK_FACT_TOPICS", "")
    fact_topic_names = {t.strip() for t in fact_topics_str.split(",") if t.strip()}
    kafka_topics = {k: v for k, v in all_topics.items() if k in fact_topic_names}

    trigger_sec = _parse_trigger_sec(driver_env.get("SPARK_FACT_TRIGGER_INTERVAL", "4 seconds"))
    pre_coalesce = int(driver_env.get("SPARK_FACT_PRE_COALESCE_PARTITIONS", "3") or "3")

    return ClusterConfig(
        worker_cores=worker_cores,
        worker_memory_mb=worker_memory_mb,
        executor_memory_mb=_parse_memory_mb(driver_env.get("SPARK_STREAM_EXECUTOR_MEMORY", "1g")),
        executor_overhead_mb=_parse_memory_mb(driver_env.get("SPARK_STREAM_EXECUTOR_MEMORY_OVERHEAD", "384m")),
        driver_memory_mb=_parse_memory_mb(driver_env.get("SPARK_STREAM_DRIVER_MEMORY", "768m")),
        driver_overhead_mb=_parse_memory_mb(driver_env.get("SPARK_STREAM_DRIVER_MEMORY_OVERHEAD", "256m")),
        kafka_topics=kafka_topics,
        kafka_retention_bytes=retention_bytes,
        kafka_retention_hours=retention_hours,
        trigger_sec=trigger_sec,
        pre_coalesce_partitions=pre_coalesce,
        compose_defaults=driver_env,
    )


# ---------------------------------------------------------------------------
# env 프로파일 로더
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class EnvProfile:
    """config/env/*.env 프로파일 설정을 담는다."""
    name: str
    eps_max: int           # 이 프로파일이 커버하는 EPS 상한 (파일 주석에서 파싱)
    shuffle_partitions: int
    max_offsets_cap: int
    min_partitions_multiplier: float
    write_partitions: int
    jdbc_batchsize: int
    safety: float


def _load_env_file(path: Path) -> dict[str, str]:
    result: dict[str, str] = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, v = line.split("=", 1)
        result[k.strip()] = v.strip()
    return result


def _parse_eps_max(path: Path) -> int:
    """파일 첫 번째 주석줄에서 EPS 상한을 파싱한다.

    예: '# HIGH: 8~10k EPS' → 10000, '# MID: 5~7k EPS' → 7000
    """
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line.startswith("#"):
            break
        m = re.search(r"(\d+)k?\s*eps", line, re.IGNORECASE)
        # '5~7k' 형태에서 마지막 숫자를 상한으로 사용
        nums = re.findall(r"(\d+)k", line, re.IGNORECASE)
        if nums:
            return int(nums[-1]) * 1000
        if m:
            return int(m.group(1)) * 1000
    return 0


def load_env_profiles() -> list[EnvProfile]:
    """config/env/*.env 파일을 읽어 EnvProfile 목록을 반환한다."""
    profiles: list[EnvProfile] = []
    for path in sorted(ENV_DIR.glob("*.env")):
        env = _load_env_file(path)
        profiles.append(EnvProfile(
            name=path.stem,
            eps_max=_parse_eps_max(path),
            shuffle_partitions=int(env.get("SPARK_STREAM_SHUFFLE_PARTITIONS", "6")),
            max_offsets_cap=int(env.get("SPARK_MAX_OFFSETS_CAP", "24000")),
            min_partitions_multiplier=float(env.get("SPARK_KAFKA_MIN_PARTITIONS_MULTIPLIER", "1")),
            write_partitions=int(env.get("SPARK_CLICKHOUSE_WRITE_PARTITIONS", "2")),
            jdbc_batchsize=int(env.get("SPARK_CLICKHOUSE_JDBC_BATCHSIZE", "5000")),
            safety=float(env.get("SPARK_MAX_OFFSETS_SAFETY", "1.1")),
        ))
    return profiles


def _resolve_profile(eps: int, profiles: list[EnvProfile]) -> EnvProfile:
    """EPS에 가장 적합한 프로파일을 반환한다.

    eps_max 오름차순으로 정렬 후 eps를 처음으로 커버하는 프로파일을 선택한다.
    """
    sorted_profiles = sorted(profiles, key=lambda p: p.eps_max)
    for p in sorted_profiles:
        if eps <= p.eps_max:
            return p
    return sorted_profiles[-1]


# ---------------------------------------------------------------------------
# profiles.yml에서 EPS 읽기
# ---------------------------------------------------------------------------

def load_profiles_eps() -> int | None:
    """log_simulator/config/profiles.yml에서 eps 값을 반환한다."""
    if not PROFILES_YML.exists():
        return None
    for line in PROFILES_YML.read_text(encoding="utf-8").splitlines():
        s = line.strip()
        if s.startswith("eps:"):
            try:
                return int(float(s.split(":", 1)[1].strip()))
            except ValueError:
                return None
    return None


# ---------------------------------------------------------------------------
# 출력 헬퍼
# ---------------------------------------------------------------------------

def _judge(ok: bool, warn_msg: str, ok_msg: str = "적절") -> str:
    return f"✅ {ok_msg}" if ok else f"⚠️  {warn_msg}"


# ---------------------------------------------------------------------------
# 계산 및 출력
# ---------------------------------------------------------------------------

def calc(eps: int, cluster: ClusterConfig, profiles: list[EnvProfile]) -> None:
    profile = _resolve_profile(eps, profiles)
    cores = cluster.total_cores
    kp = cluster.total_kafka_partitions
    t = cluster.trigger_sec

    # 1. Simulator → Kafka
    kafka_mbps = eps * AVG_EVENT_BYTES / 1024 / 1024
    retention_needed_gb = kafka_mbps * cluster.kafka_retention_hours * 3600 / 1024
    retention_cap_gb = cluster.kafka_retention_bytes / 1024 ** 3

    # 2. Kafka read
    min_parts = int(kp * profile.min_partitions_multiplier)
    max_offsets_total = int(eps * t * profile.safety)
    cap_ok = max_offsets_total <= profile.max_offsets_cap

    # 3. Spark 배치
    events_per_batch = eps * t
    raw_mb = events_per_batch * AVG_EVENT_BYTES / 1024 / 1024
    df_mb = raw_mb * 3
    jdbc_mb = profile.jdbc_batchsize * AVG_EVENT_BYTES / 1024 / 1024
    write_total_mb = jdbc_mb * profile.write_partitions
    executor_needed_mb = df_mb + write_total_mb + 200

    hr = "-" * 60
    print(hr)
    print(f"  EPS {eps:,}  →  프로파일: {profile.name}")
    print(hr)

    print("\n[1] Simulator → Kafka")
    print(f"    전송 대역              : {kafka_mbps:.1f} MB/s")
    print(f"    보존 필요 ({cluster.kafka_retention_hours}h)          : {retention_needed_gb:.1f} GB")
    print(f"    현재 cap ({retention_cap_gb:.0f} GB)       : {_judge(retention_needed_gb <= retention_cap_gb, f'{retention_needed_gb:.1f} GB > cap → KAFKA_LOG_RETENTION_BYTES 증설 필요')}")

    print("\n[2] Kafka read")
    print(f"    Kafka 파티션           : {kp}개  {dict(cluster.kafka_topics)}")
    print(f"    minPartitions (×{profile.min_partitions_multiplier:.0f})     : {min_parts}개")
    print(f"    코어 {cores}개 대비           : {min_parts}/{cores} = {min_parts/cores:.1f} 파티션/코어  {_judge(cores * 0.8 <= min_parts <= cores * 4, '파티션 수 재검토')}")
    print(f"    maxOffsets (총합)      : {max_offsets_total:,}")
    print(f"    cap ({profile.max_offsets_cap:,})         : {_judge(cap_ok, f'{max_offsets_total:,} > cap → 상위 프로파일 필요')}")

    print("\n[3] parse / normalize")
    print(f"    파티션                 : {min_parts}개 유지 (셔플 없음)")
    print(f"    배치 이벤트            : {events_per_batch:,}개")
    print(f"    raw 데이터             : {raw_mb:.1f} MB")
    print(f"    DataFrame 메모리 (~3×) : {df_mb:.0f} MB")

    print("\n[4] pre_coalesce → JDBC write")
    print(f"    pre_coalesce           : {min_parts} → {cluster.pre_coalesce_partitions}개")
    print(f"    write_partitions       : {profile.write_partitions}개  {_judge(profile.write_partitions <= cores, f'write({profile.write_partitions}) > 코어({cores})')}")
    print(f"    jdbc_batchsize         : {profile.jdbc_batchsize:,}개 = {jdbc_mb:.1f} MB/배치")
    print(f"    동시 write 총량        : {write_total_mb:.1f} MB")

    print("\n[5] Executor 메모리")
    exec_ok = executor_needed_mb < cluster.executor_memory_mb
    print(f"    실제 압력              : {executor_needed_mb:.0f} MB  (DataFrame + JDBC + JVM 기본)")
    print(f"    할당 ({cluster.executor_memory_mb:,} MB)        : {_judge(exec_ok, f'{executor_needed_mb:.0f} MB > {cluster.executor_memory_mb} MB → executor 증설 필요')}")
    print(f"    여유                   : {cluster.executor_memory_mb - executor_needed_mb:.0f} MB")

    print("\n[6] shuffle_partitions")
    sp = profile.shuffle_partitions
    print(f"    설정값                 : {sp}  {_judge(sp >= cores, f'{sp} < 코어({cores})', f'{sp}개 (dedup 미사용 시 무관)')}")

    print()


def main() -> None:
    cluster = load_cluster_config()
    profiles = load_env_profiles()

    eps_list: list[int]
    if len(sys.argv) > 1:
        try:
            eps_list = [int(a) for a in sys.argv[1:]]
        except ValueError:
            print(f"사용법: {sys.argv[0]} [EPS ...]  (예: 5000 10000)", file=sys.stderr)
            sys.exit(1)
    else:
        sim_eps = load_profiles_eps()
        eps_list = [sim_eps, sim_eps * 2] if sim_eps else [5000, 10000]

    print(f"\n{'='*60}")
    print(f"  파이프라인 용량 계산기")
    print(f"  클러스터 : 코어 {cluster.total_cores}개 {dict(cluster.worker_cores)}")
    print(f"  Kafka    : 파티션 {cluster.total_kafka_partitions}개  보존 {cluster.kafka_retention_bytes // 1024**3}GB / {cluster.kafka_retention_hours}h")
    print(f"  Spark    : executor {cluster.executor_memory_mb}MB  트리거 {cluster.trigger_sec}s  pre_coalesce {cluster.pre_coalesce_partitions}")
    print(f"  프로파일 : {[p.name for p in profiles]}")
    print(f"{'='*60}\n")

    for eps in eps_list:
        calc(eps, cluster, profiles)


if __name__ == "__main__":
    main()
