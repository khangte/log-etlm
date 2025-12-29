-- Grafana가 system.query_log 기반 대시보드를 조회할 수 있도록 권한 부여
-- NOTE: ClickHouse 버전에 따라 system.* 접근이 기본적으로 제한될 수 있다.

CREATE USER IF NOT EXISTS grafana_user IDENTIFIED BY 'grafana_pwd';

-- 기존 로그 대시보드(analytics 스키마) 조회용
GRANT SELECT ON analytics.fact_log_agg_1m TO grafana_user;
GRANT SELECT ON analytics.fact_log_agg_event_1m TO grafana_user;
GRANT SELECT ON analytics.fact_log_topic_1m TO grafana_user;
GRANT SELECT ON analytics.fact_log_latency_1m TO grafana_user;
GRANT SELECT ON analytics.fact_log_lag_1m TO grafana_user;
GRANT SELECT ON analytics.fact_log TO grafana_user;
GRANT SELECT ON analytics.dim_service TO grafana_user;
GRANT SELECT ON analytics.dim_status_code TO grafana_user;
GRANT SELECT ON analytics.dim_date TO grafana_user;
GRANT SELECT ON analytics.dim_time TO grafana_user;
GRANT SELECT ON system.parts TO grafana_user;
GRANT SELECT ON system.merges TO grafana_user;

-- 쿼리 모니터링 대시보드 조회용
GRANT SELECT ON system.query_log TO grafana_user;
