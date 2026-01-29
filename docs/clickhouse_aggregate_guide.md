# ClickHouse 집계/권한/백필 공통 가이드

Grafana/운영 지표에 필요한 **집계 테이블 + MV + 권한 + 백필**을 공통적으로 적용하는 절차입니다.
특정 패널(예: “생성 대비 적재 비율 (1m)”)뿐 아니라, **새 집계 테이블을 추가할 때마다 동일하게 사용**하세요.

## 1. 집계 테이블 + MV 적용

```bash
docker exec -i clickhouse clickhouse-client -u log_user --password log_pwd --multiquery < infra/clickhouse/sql/20_aggregates.sql
docker exec -i clickhouse clickhouse-client -u log_user --password log_pwd --multiquery < infra/clickhouse/sql/21_materialized_views.sql
```

## 2. Grafana 권한 부여

```bash
docker exec -i clickhouse clickhouse-client -u log_user --password log_pwd --multiquery < infra/clickhouse/sql/90_grants.sql
```

## 3. (선택) 과거 데이터 백필

MV는 **이후 삽입 데이터만** 자동 집계합니다. 과거 구간이 필요하면 대상 테이블에 맞춘 백필을 실행합니다.

### 3-1. 생성 대비 적재 비율(1m) 백필 예시

```bash
docker exec -i clickhouse clickhouse-client -u log_user --password log_pwd --multiquery <<'SQL'
INSERT INTO analytics.fact_event_created_stored_1m
SELECT
  toStartOfMinute(created_ts) AS bucket,
  count() AS created_cnt,
  0 AS stored_cnt
FROM analytics.fact_event
WHERE created_ts IS NOT NULL
GROUP BY bucket
UNION ALL
SELECT
  toStartOfMinute(stored_ts) AS bucket,
  0 AS created_cnt,
  count() AS stored_cnt
FROM analytics.fact_event
WHERE stored_ts IS NOT NULL
GROUP BY bucket;
SQL
```

## 4. 참고

- 버킷 기반 비율 지표는 **created/stored 버킷이 어긋나면 일시적으로 0%가 보일 수 있음**.
- 지연이 큰 구간에서 정상적으로 발생할 수 있으니, 필요한 경우 5분 윈도우 집계로 해석한다.
