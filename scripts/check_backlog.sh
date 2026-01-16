#!/usr/bin/env bash
set -euo pipefail

# Usage:
#   SUDO=sudo scripts/check_backlog.sh
#   TOPICS="logs.auth,logs.order" CHECKPOINT_DIR=/data/log-etlm/spark_checkpoints/fact_event scripts/check_backlog.sh

SUDO="${SUDO:-}"
SPARK_SERVICE="${SPARK_SERVICE:-spark}"
KAFKA_SERVICE="${KAFKA_SERVICE:-kafka}"
KAFKA_BROKER="${KAFKA_BROKER:-kafka:9092}"
CHECKPOINT_DIR="${CHECKPOINT_DIR:-/data/log-etlm/spark_checkpoints/fact_event}"
TOPICS="${TOPICS:-}"

compose_cmd=(docker compose)
if [ -n "$SUDO" ]; then
  compose_cmd=($SUDO "${compose_cmd[@]}")
fi

run_compose() {
  "${compose_cmd[@]}" "$@"
}

if [ -z "$TOPICS" ]; then
  FACT_TOPICS="$(run_compose exec -T "$SPARK_SERVICE" printenv SPARK_FACT_TOPICS 2>/dev/null || true)"
  DLQ_TOPIC="$(run_compose exec -T "$SPARK_SERVICE" printenv SPARK_DLQ_TOPIC 2>/dev/null || true)"
  TOPICS="$FACT_TOPICS"
  if [ -n "$DLQ_TOPIC" ]; then
    if [ -n "$TOPICS" ]; then
      TOPICS="$TOPICS,$DLQ_TOPIC"
    else
      TOPICS="$DLQ_TOPIC"
    fi
  fi
fi

TOPICS="${TOPICS// /}"
if [ -z "$TOPICS" ]; then
  echo "No topics found. Set TOPICS or SPARK_FACT_TOPICS."
  exit 1
fi

IFS=',' read -r -a topic_list <<< "$TOPICS"

tmp_kafka="$(mktemp)"
trap 'rm -f "$tmp_kafka"' EXIT

for topic in "${topic_list[@]}"; do
  if [ -z "$topic" ]; then
    continue
  fi
  run_compose exec -T "$KAFKA_SERVICE" sh -lc \
    "kafka-run-class kafka.tools.GetOffsetShell --broker-list $KAFKA_BROKER --topic $topic" \
    >> "$tmp_kafka" || true
done

latest_file="$(
  run_compose exec -T "$SPARK_SERVICE" sh -lc \
    "ls -1 $CHECKPOINT_DIR/offsets 2>/dev/null | grep -E '^[0-9]+$' | sort -n | tail -n 1" \
    | tr -d '\r'
)"

if [ -z "$latest_file" ]; then
  echo "No checkpoint offsets found under $CHECKPOINT_DIR/offsets"
  exit 1
fi

ckpt_raw="$(run_compose exec -T "$SPARK_SERVICE" sh -lc "cat $CHECKPOINT_DIR/offsets/$latest_file" || true)"
kafka_raw="$(cat "$tmp_kafka")"

CKPT_RAW="$ckpt_raw" KAFKA_RAW="$kafka_raw" LATEST_FILE="$latest_file" python3 - <<'PY'
import json
import os
import re

ckpt_raw = os.environ.get("CKPT_RAW", "")
kafka_raw = os.environ.get("KAFKA_RAW", "")
latest_file = os.environ.get("LATEST_FILE", "")

def parse_kafka_offsets(raw: str):
    offsets = {}
    for line in raw.splitlines():
        line = line.strip()
        if not line:
            continue
        m = re.match(r"([^:]+):([0-9]+):([0-9]+)$", line)
        if not m:
            continue
        topic, part, off = m.groups()
        offsets.setdefault(topic, {})[int(part)] = int(off)
    return offsets

def _merge_offsets(end_offsets, end):
    if end is None:
        return
    if isinstance(end, str):
        try:
            end = json.loads(end)
        except json.JSONDecodeError:
            return
    if isinstance(end, dict):
        for key in ("topicPartitions", "offsets", "endOffset"):
            if key in end and isinstance(end[key], (dict, list)):
                end = end[key]
                break
    if isinstance(end, dict):
        for topic, parts in end.items():
            if isinstance(parts, dict):
                for part, off in parts.items():
                    try:
                        end_offsets.setdefault(topic, {})[int(part)] = int(off)
                    except (TypeError, ValueError):
                        continue
            elif isinstance(parts, list):
                for item in parts:
                    if not isinstance(item, dict):
                        continue
                    try:
                        part = int(item.get("partition"))
                        off = int(item.get("offset"))
                    except (TypeError, ValueError):
                        continue
                    end_offsets.setdefault(topic, {})[part] = off
    elif isinstance(end, list):
        for item in end:
            if not isinstance(item, dict):
                continue
            topic = item.get("topic") or item.get("topicName")
            part = item.get("partition") or item.get("partitionId")
            off = item.get("offset") or item.get("endOffset")
            try:
                if topic is None:
                    continue
                part = int(part)
                off = int(off)
            except (TypeError, ValueError):
                continue
            end_offsets.setdefault(topic, {})[part] = off

def parse_checkpoint_offsets(raw: str):
    lines = [l.strip() for l in raw.splitlines() if l.strip()]
    lines = [l for l in lines if not l.startswith("v")]
    if not lines:
        return {}, None, None
    record = None
    try:
        record = json.loads(lines[-1])
    except json.JSONDecodeError:
        try:
            record = json.loads("\n".join(lines))
        except json.JSONDecodeError:
            return {}, None, lines[-1]
    end_offsets = {}
    sources = record.get("sources") if isinstance(record, dict) else None
    if sources:
        for src in sources:
            if not isinstance(src, dict):
                continue
            _merge_offsets(end_offsets, src.get("endOffset"))
            _merge_offsets(end_offsets, src.get("offsets"))
    elif isinstance(record, dict):
        _merge_offsets(end_offsets, record)
    return end_offsets, record.get("batchId"), lines[-1]

kafka_offsets = parse_kafka_offsets(kafka_raw)
ckpt_offsets, batch_id, ckpt_line = parse_checkpoint_offsets(ckpt_raw)

print(f"checkpoint_offsets_file={latest_file} batch_id={batch_id}")
print(f"kafka_topics={','.join(sorted(kafka_offsets.keys()))}")
print("")

topic_lag_sum = {}
topic_lag_max = {}
missing = 0
rows = []
for topic, parts in kafka_offsets.items():
    for part, latest in parts.items():
        end = ckpt_offsets.get(topic, {}).get(part)
        if end is None:
            missing += 1
            continue
        lag = max(0, latest - end)
        rows.append((topic, part, latest, end, lag))
        topic_lag_sum[topic] = topic_lag_sum.get(topic, 0) + lag
        topic_lag_max[topic] = max(topic_lag_max.get(topic, 0), lag)

rows.sort()
print("topic\tpartition\tkafka\tcheckpoint\tlag")
for topic, part, latest, end, lag in rows:
    print(f"{topic}\t{part}\t{latest}\t{end}\t{lag}")

print("")
print("topic\tlag_sum\tlag_max")
total = 0
for topic in sorted(topic_lag_sum.keys()):
    total += topic_lag_sum[topic]
    print(f"{topic}\t{topic_lag_sum[topic]}\t{topic_lag_max[topic]}")

print("")
print(f"total_lag={total} missing_partitions={missing}")
if not ckpt_offsets:
    sample = (ckpt_line or "")[:200]
    print(f"warning=checkpoint_offsets_empty sample={sample!r}")
print("")
print("[checklist]")
print("- backlog > 0: 소비가 유입을 따라잡지 못함 (Spark 처리량 또는 maxOffsetsPerTrigger 점검)")
print("- backlog = 0, publish 지연 급증: ingest_ts 자체가 과거일 가능성 (producer 지연/재전송)")
print("- sink 지연 동반: ClickHouse insert 지연/compaction 확인")
PY
