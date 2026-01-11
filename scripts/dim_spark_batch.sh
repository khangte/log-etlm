#!/usr/bin/env bash
# Spark 배치 컨테이너를 실행해 dim 테이블을 갱신한다.
# 중복 실행을 막기 위해 파일 잠금(flock)을 사용한다.
set -euo pipefail
cd /home/kang/log-etlm

# 중복 실행 방지
exec 9>/tmp/dim_batch.lock
flock -n 9 || exit 0

docker compose run --rm spark-batch
