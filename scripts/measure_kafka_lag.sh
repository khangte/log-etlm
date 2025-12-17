#!/usr/bin/env bash
set -euo pipefail

KAFKA_CONTAINER="${KAFKA_CONTAINER:-kafka}"
BROKER="${BROKER:-kafka:9092}"
SPARK_CONTAINER="${SPARK_CONTAINER:-spark}"
CHECKPOINT_DIR="${CHECKPOINT_DIR:-/data/spark_checkpoints/fact_log}"

# Spark Structured Streaming (Kafka source) typically does NOT commit offsets to Kafka consumer groups.
# Offsets are stored in the checkpoint directory. This script computes lag as:
#   lag = kafka_end_offset - spark_checkpoint_offset

latest_batch_id="$(
  docker exec "${SPARK_CONTAINER}" bash -lc \
    "ls -1 '${CHECKPOINT_DIR}/offsets' 2>/dev/null | grep -E '^[0-9]+$' | sort -n | tail -1"
)"

if [[ -z "${latest_batch_id}" ]]; then
  echo "[measure_kafka_lag] no checkpoint offset files found at ${CHECKPOINT_DIR}/offsets" >&2
  exit 1
fi

checkpoint_file="${CHECKPOINT_DIR}/offsets/${latest_batch_id}"

checkpoint_payload="$(
  docker exec "${SPARK_CONTAINER}" bash -lc "cat '${checkpoint_file}'"
)"

end_offsets="$(
  docker exec "${KAFKA_CONTAINER}" bash -lc \
    "kafka-run-class kafka.tools.GetOffsetShell --broker-list '${BROKER}' --topic 'logs\\..*' --time -1"
)"

tmp_checkpoint="$(mktemp)"
tmp_end_offsets="$(mktemp)"
trap 'rm -f "${tmp_checkpoint}" "${tmp_end_offsets}"' EXIT

printf "%s" "${checkpoint_payload}" >"${tmp_checkpoint}"
printf "%s" "${end_offsets}" >"${tmp_end_offsets}"

python3 - "${checkpoint_file}" "${tmp_checkpoint}" "${tmp_end_offsets}" <<'PY'
import json
import sys

checkpoint_file = sys.argv[1]
checkpoint_path = sys.argv[2]
end_offsets_path = sys.argv[3]

with open(checkpoint_path, "r", encoding="utf-8") as f:
    checkpoint_payload = f.read()

with open(end_offsets_path, "r", encoding="utf-8") as f:
    end_offsets_text = f.read()

lines = [ln for ln in checkpoint_payload.splitlines() if ln.strip()]
if len(lines) < 3:
    print(f"[measure_kafka_lag] unexpected checkpoint format: {checkpoint_file}", file=sys.stderr)
    sys.exit(1)

meta = json.loads(lines[1])
committed = json.loads(lines[2])

end_offsets = {}
for ln in end_offsets_text.splitlines():
    ln = ln.strip()
    if not ln:
        continue
    # topic:partition:offset
    parts = ln.split(":")
    if len(parts) != 3:
        continue
    topic, part, off = parts
    end_offsets.setdefault(topic, {})[int(part)] = int(off)

expected_topics = ["logs.auth", "logs.error", "logs.order", "logs.payment", "logs.notify"]
missing = [t for t in expected_topics if t not in end_offsets]
if missing:
    print(f"[measure_kafka_lag] WARNING: missing end offsets for topics: {missing}", file=sys.stderr)

for t in expected_topics:
    parts = end_offsets.get(t, {})
    if parts and len(parts) < 8:
        print(f"[measure_kafka_lag] WARNING: topic {t} partitions={len(parts)} (expected 8)", file=sys.stderr)

def sum_lag_for_topic(topic: str) -> int:
    topic_committed = committed.get(topic, {})
    topic_end = end_offsets.get(topic, {})
    lag = 0
    for p, end in topic_end.items():
        c = int(topic_committed.get(str(p), 0))
        lag += max(0, end - c)
    return lag

topics = sorted(set(end_offsets.keys()) | set(committed.keys()))
per_topic = [(t, sum_lag_for_topic(t)) for t in topics if t.startswith("logs.")]
total_lag = sum(v for _, v in per_topic)

batch_ts_ms = int(meta.get("batchTimestampMs", 0))
print(f"checkpoint={checkpoint_file} batchTimestampMs={batch_ts_ms} total_lag={total_lag}")
for t, lag in sorted(per_topic, key=lambda x: x[0]):
    print(f"{t}\tlag={lag}")
PY
