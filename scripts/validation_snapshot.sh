#!/usr/bin/env bash
set -uo pipefail

# 용도: 검증 실행 동안 진단 결과를 주기적으로 파일에 남기고, 종료(Ctrl-C/kill) 시 최종 산출물을 수집한다.
# 사용법: bash scripts/validation_snapshot.sh [간격초=300]
#   백그라운드: nohup bash scripts/validation_snapshot.sh > /dev/null 2>&1 &   (종료: kill <pid>)
# 산출물: $VALIDATION_DIR/<UTC시각>/ 아래 snapshots.log, docker_stats.log, batch_timing.log, compose.log

INTERVAL="${1:-300}"
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
OUT="${VALIDATION_DIR:-/data/log-etlm/validation}/$(date -u +%Y%m%dT%H%MZ)"
mkdir -p "$OUT"
cd "$ROOT"

collect() {
  echo "[validation] 최종 수집 → $OUT"
  { echo "=== final $(date -u +%FT%TZ)"; bash scripts/diag.sh all; } >> "$OUT/final_diag.log" 2>&1
  cp /data/log-etlm/spark-events/batch_timing.log "$OUT/" 2>/dev/null || echo "[validation] batch_timing.log 없음"
  docker compose logs --no-color --timestamps > "$OUT/compose.log" 2>&1
  git rev-parse HEAD > "$OUT/git_head.txt" 2>/dev/null
}
trap collect EXIT
trap 'exit 0' INT TERM

echo "[validation] 간격 ${INTERVAL}s, 출력 $OUT"
while true; do
  {
    echo "=== snapshot $(date -u +%FT%TZ)"
    bash scripts/diag.sh all
    python3 scripts/kafka_spark_lag.py
  } >> "$OUT/snapshots.log" 2>&1
  { echo "=== $(date -u +%FT%TZ)"; docker stats --no-stream; } >> "$OUT/docker_stats.log" 2>&1
  sleep "$INTERVAL" &
  wait $!  # sleep을 백그라운드로 둬야 TERM 시그널에 즉시 반응
done
