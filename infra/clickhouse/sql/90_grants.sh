#!/bin/bash
set -e

clickhouse-client --host localhost \
  --user "${CLICKHOUSE_USER}" \
  --password "${CLICKHOUSE_PASSWORD}" \
  --query "CREATE USER IF NOT EXISTS grafana_user IDENTIFIED BY '${CLICKHOUSE_GRAFANA_PASSWORD}'"

clickhouse-client --host localhost \
  --user "${CLICKHOUSE_USER}" \
  --password "${CLICKHOUSE_PASSWORD}" \
  --query "ALTER USER IF EXISTS grafana_user SETTINGS
    max_memory_usage = 1073741824,
    max_bytes_before_external_group_by = 805306368,
    max_bytes_before_external_sort = 536870912,
    max_threads = 4,
    priority = 10,
    max_execution_time = 8"

clickhouse-client --host localhost \
  --user "${CLICKHOUSE_USER}" \
  --password "${CLICKHOUSE_PASSWORD}" \
  --multiquery "
GRANT SELECT ON analytics.fact_event_agg_1m TO grafana_user;
GRANT SELECT ON analytics.fact_event_agg_10s TO grafana_user;
GRANT SELECT ON analytics.fact_event_latency_1m TO grafana_user;
GRANT SELECT ON analytics.fact_event_latency_10s TO grafana_user;
GRANT SELECT ON analytics.fact_event_lag_1m TO grafana_user;
GRANT SELECT ON analytics.fact_event_latency_service_1m TO grafana_user;
GRANT SELECT ON analytics.fact_event_latency_stage_10s TO grafana_user;
GRANT SELECT ON analytics.fact_event_dlq_agg_1m TO grafana_user;
GRANT SELECT ON analytics.fact_event_created_stored_1m TO grafana_user;
GRANT SELECT ON analytics.fact_event_freshness_1m TO grafana_user;
GRANT SELECT ON analytics.fact_event TO grafana_user;
GRANT SELECT ON analytics.fact_event_dlq TO grafana_user;
GRANT SELECT ON analytics.dim_service TO grafana_user;
GRANT SELECT ON analytics.dim_status_code TO grafana_user;
GRANT SELECT ON analytics.dim_date TO grafana_user;
GRANT SELECT ON analytics.dim_time TO grafana_user;
GRANT SELECT ON analytics.dim_user TO grafana_user;
GRANT SELECT ON system.parts TO grafana_user;
GRANT SELECT ON system.merges TO grafana_user;
GRANT SELECT ON system.query_log TO grafana_user;
"
