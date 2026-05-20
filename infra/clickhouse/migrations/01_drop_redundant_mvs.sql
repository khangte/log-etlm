-- ClickHouse MV 중복 제거 마이그레이션 (2026-05-20)
--
-- 적용 방법:
--   source .env && docker exec clickhouse \
--     clickhouse-client -u log_user --password "$CLICKHOUSE_PASSWORD" \
--     --multiquery < infra/clickhouse/migrations/01_drop_redundant_mvs.sql
--
-- mv_fact_event_latency_1m:
--   mv_fact_event_latency_service_1m이 동일 컬럼(e2e_state, spark_processing_state 등)을
--   service 차원 포함하여 제공하므로 중복. Grafana 쿼리는 fact_event_latency_service_1m으로 이전.
DROP VIEW IF EXISTS analytics.mv_fact_event_latency_1m;

-- mv_fact_event_latency_10s:
--   mv_fact_event_latency_stage_10s가 e2e_state를 포함하여 제공하므로 중복.
--   Grafana 쿼리는 fact_event_latency_stage_10s로 이전.
DROP VIEW IF EXISTS analytics.mv_fact_event_latency_10s;
