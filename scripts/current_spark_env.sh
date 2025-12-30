#!/usr/bin/env bash
set -euo pipefail

# 현재 적용된 Spark env 프로파일을 출력한다.
# 컨테이너에 실제 적용된 값을 우선 확인하고, 예시 env와 일치하면 그 프로파일명을 출력한다.

ENV_DIR="/home/kang/log-etlm/env"
if ! docker ps --format '{{.Names}}' | grep -qx "spark"; then
  echo "spark not running"
  exit 1
fi

declare -A CURRENT
while IFS='=' read -r key value; do
  CURRENT["$key"]="$value"
done < <(docker exec -i spark env | grep '^SPARK_')

for profile in low mid high; do
  env_file="${ENV_DIR}/${profile}.env.example"
  if [ ! -f "$env_file" ]; then
    continue
  fi
  matched=1
  while IFS='=' read -r key value; do
    [ -z "$key" ] && continue
    if [ "${CURRENT[$key]+set}" != "set" ] || [ "${CURRENT[$key]}" != "$value" ]; then
      matched=0
      break
    fi
  done < <(grep '^SPARK_' "$env_file")
  if [ "$matched" -eq 1 ]; then
    echo "$profile"
    exit 0
  fi
done

echo "custom"
