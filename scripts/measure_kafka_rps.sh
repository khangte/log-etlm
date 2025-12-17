#!/usr/bin/env bash
set -euo pipefail

KAFKA_CONTAINER="${KAFKA_CONTAINER:-kafka}"
BROKER="${BROKER:-kafka:9092}"
INTERVAL_SEC="${1:-60}"

TOPICS=(
  "logs.auth"
  "logs.error"
  "logs.order"
  "logs.payment"
  "logs.notify"
)

snapshot_offsets() {
  # Query all logs.* topics in one call (faster than per-topic calls).
  docker exec "${KAFKA_CONTAINER}" bash -lc \
    "kafka-run-class kafka.tools.GetOffsetShell --broker-list '${BROKER}' --topic 'logs\\..*' --time -1"
}

declare -A start end delta

start_ts="$(date +%s)"
while IFS=: read -r topic _partition offset; do
  [[ -z "${topic:-}" ]] && continue
  [[ -z "${offset:-}" ]] && continue
  start["$topic"]="$(( ${start["$topic"]:-0} + offset ))"
done < <(snapshot_offsets)

if [[ "${#start[@]}" -eq 0 ]]; then
  echo "[measure_kafka_rps] failed to read offsets (no output). check kafka container/broker/topic names." >&2
  exit 1
fi

sleep "${INTERVAL_SEC}"

end_ts="$(date +%s)"
while IFS=: read -r topic _partition offset; do
  [[ -z "${topic:-}" ]] && continue
  [[ -z "${offset:-}" ]] && continue
  end["$topic"]="$(( ${end["$topic"]:-0} + offset ))"
done < <(snapshot_offsets)

for t in "${TOPICS[@]}"; do
  delta["$t"]="$(( ${end["$t"]:-0} - ${start["$t"]:-0} ))"
done

dt="$(( end_ts - start_ts ))"
total=0
for t in "${TOPICS[@]}"; do
  total="$(( total + delta["$t"] ))"
done

python3 - <<PY
dt=${dt}
total=${total}
d_auth=${delta["logs.auth"]}
d_error=${delta["logs.error"]}
d_order=${delta["logs.order"]}
d_payment=${delta["logs.payment"]}
d_notify=${delta["logs.notify"]}
print(f"dt={dt}s total={total} rps={total/dt:.1f} (auth={d_auth} error={d_error} order={d_order} payment={d_payment} notify={d_notify})")
PY
