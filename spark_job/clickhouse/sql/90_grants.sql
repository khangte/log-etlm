-- Grafana read access
CREATE USER IF NOT EXISTS grafana_user IDENTIFIED BY 'grafana_pwd';

GRANT SELECT ON analytics.fact_event_agg_1m TO grafana_user;
-- GRANT SELECT ON analytics.fact_event_latency_1m TO grafana_user;
GRANT SELECT ON analytics.fact_event_lag_1m TO grafana_user;
GRANT SELECT ON analytics.fact_event TO grafana_user;
GRANT SELECT ON analytics.fact_event_dlq TO grafana_user;

-- GRANT SELECT ON analytics.dim_service TO grafana_user;
-- GRANT SELECT ON analytics.dim_date TO grafana_user;
-- GRANT SELECT ON analytics.dim_time TO grafana_user;
-- GRANT SELECT ON analytics.dim_user TO grafana_user;

GRANT SELECT ON system.parts TO grafana_user;
GRANT SELECT ON system.merges TO grafana_user;
GRANT SELECT ON system.query_log TO grafana_user;
