#!/usr/bin/env bash
set -euo pipefail

# End-to-end measurement without Kafka consumer groups:
# - Produced EPS: Kafka end offsets delta
# - Consumed EPS: Spark checkpoint offsets delta
# - Lag: (Kafka end offsets - Spark checkpoint offsets)

KAFKA_CONTAINER="${KAFKA_CONTAINER:-kafka}"
BROKER="${BROKER:-kafka:9092}"
SPARK_CONTAINER="${SPARK_CONTAINER:-spark}"
CHECKPOINT_DIR="${CHECKPOINT_DIR:-/data/spark_checkpoints/fact_log}"
INTERVAL_SEC="${1:-30}"

latest_checkpoint_file() {
  docker exec "${SPARK_CONTAINER}" bash -lc \
    "ls -1 '${CHECKPOINT_DIR}/offsets' 2>/dev/null | grep -E '^[0-9]+$' | sort -n | tail -1"
}

read_checkpoint_offsets() {
  local batch_id="$1"
  docker exec "${SPARK_CONTAINER}" bash -lc "cat '${CHECKPOINT_DIR}/offsets/${batch_id}'"
}

read_end_offsets() {
  docker exec "${KAFKA_CONTAINER}" bash -lc \
    "kafka-run-class kafka.tools.GetOffsetShell --broker-list '${BROKER}' --topic 'logs\\..*' --time -1"
}

batch0="$(latest_checkpoint_file)"
chk0="$(read_checkpoint_offsets "${batch0}")"
end0="$(read_end_offsets)"
ts0="$(date +%s)"

sleep "${INTERVAL_SEC}"

batch1="$(latest_checkpoint_file)"
chk1="$(read_checkpoint_offsets "${batch1}")"
end1="$(read_end_offsets)"
ts1="$(date +%s)"

tmp_chk0="$(mktemp)"; tmp_chk1="$(mktemp)"; tmp_end0="$(mktemp)"; tmp_end1="$(mktemp)"
trap 'rm -f "${tmp_chk0}" "${tmp_chk1}" "${tmp_end0}" "${tmp_end1}"' EXIT
printf "%s" "${chk0}" >"${tmp_chk0}"
printf "%s" "${chk1}" >"${tmp_chk1}"
printf "%s" "${end0}" >"${tmp_end0}"
printf "%s" "${end1}" >"${tmp_end1}"

python3 - "${batch0}" "${batch1}" "${ts0}" "${ts1}" "${tmp_chk0}" "${tmp_chk1}" "${tmp_end0}" "${tmp_end1}" <<'PY'
import json
import sys

batch0, batch1 = sys.argv[1], sys.argv[2]
ts0, ts1 = int(sys.argv[3]), int(sys.argv[4])
chk0_path, chk1_path = sys.argv[5], sys.argv[6]
end0_path, end1_path = sys.argv[7], sys.argv[8]

def load_checkpoint(path: str) -> dict:
    with open(path, "r", encoding="utf-8") as f:
        lines = [ln for ln in f.read().splitlines() if ln.strip()]
    if len(lines) < 3:
        raise RuntimeError(f"unexpected checkpoint format: {path}")
    meta = json.loads(lines[1])
    offsets = json.loads(lines[2])
    return {"meta": meta, "offsets": offsets}

def load_end_offsets(path: str) -> dict:
    end_offsets: dict[str, dict[int, int]] = {}
    with open(path, "r", encoding="utf-8") as f:
        for ln in f:
            ln = ln.strip()
            if not ln:
                continue
            parts = ln.split(":")
            if len(parts) != 3:
                continue
            topic, part, off = parts
            end_offsets.setdefault(topic, {})[int(part)] = int(off)
    return end_offsets

def sum_delta(a: dict, b: dict) -> int:
    total = 0
    for topic, parts_b in b.items():
        if not topic.startswith("logs."):
            continue
        parts_a = a.get(topic, {})
        for p, off_b in parts_b.items():
            off_a = parts_a.get(p, 0)
            total += max(0, off_b - off_a)
    return total

def lag(end: dict, committed: dict) -> int:
    total = 0
    for topic, parts_end in end.items():
        if not topic.startswith("logs."):
            continue
        parts_c = committed.get(topic, {})
        for p, off_e in parts_end.items():
            off_c = int(parts_c.get(str(p), 0))
            total += max(0, off_e - off_c)
    return total

dt = max(1, ts1 - ts0)

chk0 = load_checkpoint(chk0_path)
chk1 = load_checkpoint(chk1_path)
end0 = load_end_offsets(end0_path)
end1 = load_end_offsets(end1_path)

comm0 = chk0["offsets"]
comm1 = chk1["offsets"]

produced = sum_delta(end0, end1)
consumed = sum_delta(
    {t: {int(p): int(v) for p, v in parts.items()} for t, parts in comm0.items() if t.startswith("logs.")},
    {t: {int(p): int(v) for p, v in parts.items()} for t, parts in comm1.items() if t.startswith("logs.")},
)

lag0 = lag(end0, comm0)
lag1 = lag(end1, comm1)

print(f"dt={dt}s checkpoint_batch={batch0}->{batch1}")
print(f"produced={produced} eps={produced/dt:.1f}")
print(f"consumed={consumed} eps={consumed/dt:.1f}")
print(f"lag0={lag0} lag1={lag1} delta={lag1-lag0}")
PY
