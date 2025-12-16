#!/usr/bin/env bash
set -euo pipefail

KAFKA_CONTAINER="${KAFKA_CONTAINER:-kafka}"
BROKER="${BROKER:-kafka:9092}"
INTERVAL_SEC="${1:-60}"

TOPICS=(
  "logs.auth"
  "logs.order"
  "logs.payment"
  "logs.notify"
)

get_offsets() {
  local topic="$1"
  docker exec "${KAFKA_CONTAINER}" kafka-run-class kafka.tools.GetOffsetShell \
    --broker-list "${BROKER}" --topic "${topic}" --time -1 2>/dev/null \
    | awk -F: '{sum+=$3} END{print sum+0}'
}

declare -A start end delta

start_ts="$(date +%s)"
for t in "${TOPICS[@]}"; do
  start["$t"]="$(get_offsets "$t")"
done

sleep "${INTERVAL_SEC}"

end_ts="$(date +%s)"
for t in "${TOPICS[@]}"; do
  end["$t"]="$(get_offsets "$t")"
  delta["$t"]="$(( end["$t"] - start["$t"] ))"
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
d_order=${delta["logs.order"]}
d_payment=${delta["logs.payment"]}
d_notify=${delta["logs.notify"]}
print(f"dt={dt}s total={total} rps={total/dt:.1f} (auth={d_auth} order={d_order} payment={d_payment} notify={d_notify})")
PY
