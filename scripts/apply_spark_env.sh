#!/usr/bin/env bash
set -euo pipefail

# 용도: Spark env 프로파일을 적용하고 spark-driver만 재기동한다.

# 사용법: ./scripts/apply_spark_env.sh {low|mid|high}
# config/env/<profile>.env.example을 적용하고 spark 서비스만 재기동한다.

PROFILE="${1:-}"
if [ -z "$PROFILE" ]; then
  echo "usage: $0 {low|mid|high}"
  exit 1
fi

ENV_FILE="/home/kang/log-etlm/config/env/${PROFILE}.env.example"
if [ ! -f "$ENV_FILE" ]; then
  echo "env file not found: $ENV_FILE"
  exit 1
fi

docker compose --env-file "$ENV_FILE" up -d --force-recreate --no-deps spark-driver
echo "applied env profile: $PROFILE ($ENV_FILE)"
