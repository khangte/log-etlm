#!/usr/bin/env bash
set -euo pipefail

# ClickHouse 지연 p95(ms)를 보고 Spark env 프로파일을 자동 전환한다.
# 필요 시 임계값/윈도우를 환경변수로 조정한다.

cd /home/kang/log-etlm

WINDOW_MIN="${WINDOW_MIN:-5}"
STATE_FILE="${STATE_FILE:-/tmp/spark_env_profile}"

# 임계값(ms) - 히스테리시스 포함
MID_TO_LOW="${MID_TO_LOW:-2500}"
LOW_RECOVER="${LOW_RECOVER:-1800}"
MID_TO_HIGH="${MID_TO_HIGH:-1200}"
HIGH_TO_MID="${HIGH_TO_MID:-1600}"

if [ -f "$STATE_FILE" ]; then
  read -r CURRENT_PROFILE < "$STATE_FILE"
else
  CURRENT_PROFILE="mid"
fi

P95_MS="$(
  docker exec -i clickhouse clickhouse-client -u log_user --password log_pwd --format=TSV \
    --query "SELECT if(count()=0, 0, quantileTDigestMerge(0.95)(sink_state)) \
             FROM analytics.fact_log_latency_1m \
             WHERE bucket >= now() - INTERVAL ${WINDOW_MIN} MINUTE"
)"

if [ -z "$P95_MS" ]; then
  echo "no data for latency window=${WINDOW_MIN}m"
  exit 0
fi

P95_MS_INT="$(printf '%.0f' "$P95_MS")"
TARGET_PROFILE="$CURRENT_PROFILE"

case "$CURRENT_PROFILE" in
  low)
    if [ "$P95_MS_INT" -lt "$LOW_RECOVER" ]; then
      TARGET_PROFILE="mid"
    fi
    ;;
  mid)
    if [ "$P95_MS_INT" -gt "$MID_TO_LOW" ]; then
      TARGET_PROFILE="low"
    elif [ "$P95_MS_INT" -lt "$MID_TO_HIGH" ]; then
      TARGET_PROFILE="high"
    fi
    ;;
  high)
    if [ "$P95_MS_INT" -gt "$HIGH_TO_MID" ]; then
      TARGET_PROFILE="mid"
    fi
    ;;
  *)
    TARGET_PROFILE="mid"
    ;;
esac

if [ "$TARGET_PROFILE" != "$CURRENT_PROFILE" ]; then
  ./scripts/apply_spark_env.sh "$TARGET_PROFILE"
  echo "$TARGET_PROFILE" > "$STATE_FILE"
  echo "switched: $CURRENT_PROFILE -> $TARGET_PROFILE (p95=${P95_MS_INT}ms)"
else
  echo "keep: $CURRENT_PROFILE (p95=${P95_MS_INT}ms)"
fi
