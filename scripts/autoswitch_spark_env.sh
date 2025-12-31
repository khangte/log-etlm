#!/usr/bin/env bash
set -euo pipefail

# ClickHouse 지연 p95(ms)를 보고 Spark env 프로파일을 자동 전환한다.
# 필요 시 임계값/윈도우를 환경변수로 조정한다.

cd /home/kang/log-etlm

WINDOW_MIN="${WINDOW_MIN:-5}"
STATE_FILE="${STATE_FILE:-/tmp/spark_env_profile}"
STREAK_FILE="${STREAK_FILE:-/tmp/spark_env_streak}"
LAST_SWITCH_FILE="${LAST_SWITCH_FILE:-/tmp/spark_env_last_switch}"

# 지표 선택: max(=max[sink,process]) | process | sink | e2e
P95_MODE="${P95_MODE:-max}"

# 연속 조건/쿨다운
SCALE_UP_STREAK="${SCALE_UP_STREAK:-2}"
SCALE_DOWN_STREAK="${SCALE_DOWN_STREAK:-3}"
COOLDOWN_SEC="${COOLDOWN_SEC:-600}"

# 임계값(ms) - 지연이 높을수록 scale-up, 낮을수록 scale-down (히스테리시스 포함)
MID_TO_LOW="${MID_TO_LOW:-1200}"
LOW_RECOVER="${LOW_RECOVER:-1600}"
MID_TO_HIGH="${MID_TO_HIGH:-2500}"
HIGH_TO_MID="${HIGH_TO_MID:-2000}"

if [ -f "$STATE_FILE" ]; then
  read -r CURRENT_PROFILE < "$STATE_FILE"
else
  CURRENT_PROFILE="mid"
fi

P95_ROW="$(
  docker exec -i clickhouse clickhouse-client -u log_user --password log_pwd --format=TSV \
    --query "SELECT \
               if(count()=0, 0, quantileTDigestMerge(0.95)(sink_state)) AS sink_p95, \
               if(count()=0, 0, quantileTDigestMerge(0.95)(process_state)) AS process_p95, \
               if(count()=0, 0, quantileTDigestMerge(0.95)(e2e_state)) AS e2e_p95, \
               if(count()=0, 0, greatest( \
                 quantileTDigestMerge(0.95)(sink_state), \
                 quantileTDigestMerge(0.95)(process_state) \
               )) AS p95_max \
             FROM analytics.fact_log_latency_1m \
             WHERE bucket >= now() - INTERVAL ${WINDOW_MIN} MINUTE"
)"

if [ -z "$P95_ROW" ]; then
  echo "no data for latency window=${WINDOW_MIN}m"
  exit 0
fi

read -r P95_SINK P95_PROCESS P95_E2E P95_MAX <<< "$P95_ROW"
case "$P95_MODE" in
  process)
    P95_MS="$P95_PROCESS"
    ;;
  sink)
    P95_MS="$P95_SINK"
    ;;
  e2e)
    P95_MS="$P95_E2E"
    ;;
  max)
    P95_MS="$P95_MAX"
    ;;
  *)
    echo "invalid P95_MODE=$P95_MODE (use: max|process|sink|e2e)"
    exit 1
    ;;
esac
P95_MS_INT="$(printf '%.0f' "$P95_MS")"
TARGET_PROFILE="$CURRENT_PROFILE"

UP_STREAK=0
DOWN_STREAK=0
if [ -f "$STREAK_FILE" ]; then
  read -r UP_STREAK DOWN_STREAK < "$STREAK_FILE" || true
fi

UP_TRIGGERED=0
DOWN_TRIGGERED=0

case "$CURRENT_PROFILE" in
  low)
    if [ "$P95_MS_INT" -gt "$LOW_RECOVER" ]; then
      TARGET_PROFILE="mid"
      UP_TRIGGERED=1
    fi
    ;;
  mid)
    if [ "$P95_MS_INT" -gt "$MID_TO_HIGH" ]; then
      TARGET_PROFILE="high"
      UP_TRIGGERED=1
    elif [ "$P95_MS_INT" -lt "$MID_TO_LOW" ]; then
      TARGET_PROFILE="low"
      DOWN_TRIGGERED=1
    fi
    ;;
  high)
    if [ "$P95_MS_INT" -lt "$HIGH_TO_MID" ]; then
      TARGET_PROFILE="mid"
      DOWN_TRIGGERED=1
    fi
    ;;
  *)
    TARGET_PROFILE="mid"
    ;;
esac

if [ "$UP_TRIGGERED" -eq 1 ]; then
  UP_STREAK=$((UP_STREAK + 1))
  DOWN_STREAK=0
elif [ "$DOWN_TRIGGERED" -eq 1 ]; then
  DOWN_STREAK=$((DOWN_STREAK + 1))
  UP_STREAK=0
else
  UP_STREAK=0
  DOWN_STREAK=0
fi

printf "%s %s\n" "$UP_STREAK" "$DOWN_STREAK" > "$STREAK_FILE"

if [ "$TARGET_PROFILE" != "$CURRENT_PROFILE" ]; then
  if [ "$UP_TRIGGERED" -eq 1 ] && [ "$UP_STREAK" -lt "$SCALE_UP_STREAK" ]; then
    echo "pending scale-up: $CURRENT_PROFILE -> $TARGET_PROFILE (streak=${UP_STREAK}/${SCALE_UP_STREAK}, p95=${P95_MS_INT}ms mode=${P95_MODE})"
    exit 0
  fi
  if [ "$DOWN_TRIGGERED" -eq 1 ] && [ "$DOWN_STREAK" -lt "$SCALE_DOWN_STREAK" ]; then
    echo "pending scale-down: $CURRENT_PROFILE -> $TARGET_PROFILE (streak=${DOWN_STREAK}/${SCALE_DOWN_STREAK}, p95=${P95_MS_INT}ms mode=${P95_MODE})"
    exit 0
  fi
  NOW="$(date +%s)"
  LAST_SWITCH=0
  if [ -f "$LAST_SWITCH_FILE" ]; then
    read -r LAST_SWITCH < "$LAST_SWITCH_FILE" || true
  fi
  if [ "$COOLDOWN_SEC" -gt 0 ] && [ $((NOW - LAST_SWITCH)) -lt "$COOLDOWN_SEC" ]; then
    echo "cooldown active: $CURRENT_PROFILE -> $TARGET_PROFILE (wait ${COOLDOWN_SEC}s, p95=${P95_MS_INT}ms mode=${P95_MODE})"
    exit 0
  fi

  ./scripts/apply_spark_env.sh "$TARGET_PROFILE"
  echo "$TARGET_PROFILE" > "$STATE_FILE"
  echo "$NOW" > "$LAST_SWITCH_FILE"
  printf "switched: %s -> %s (p95=%sms mode=%s sink=%sms process=%sms e2e=%sms)\n" \
    "$CURRENT_PROFILE" "$TARGET_PROFILE" "$P95_MS_INT" "$P95_MODE" "$P95_SINK" "$P95_PROCESS" "$P95_E2E"
else
  printf "keep: %s (p95=%sms mode=%s sink=%sms process=%sms e2e=%sms)\n" \
    "$CURRENT_PROFILE" "$P95_MS_INT" "$P95_MODE" "$P95_SINK" "$P95_PROCESS" "$P95_E2E"
fi
