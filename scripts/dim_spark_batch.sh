#!/usr/bin/env bash
set -euo pipefail
cd /home/kang/log-etlm

# 중복 실행 방지
exec 9>/tmp/dim_batch.lock
flock -n 9 || exit 0

# 필요 시 기간 조절
export DIM_BATCH_LOOKBACK_DAYS=1

docker compose run --rm spark-batch
