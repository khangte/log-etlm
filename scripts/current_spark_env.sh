#!/usr/bin/env bash
set -euo pipefail

# 현재 적용된 Spark env 프로파일을 출력한다.

STATE_FILE="${STATE_FILE:-/tmp/spark_env_profile}"
if [ -f "$STATE_FILE" ]; then
  cat "$STATE_FILE"
else
  echo "mid"
fi
