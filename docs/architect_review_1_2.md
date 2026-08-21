# 우선순위 1·2번 설계 검토 요청 (docs/overengineering_audit.md 미처리 항목)

## 1. stdlib 테스트 harness 전환 (audit line 13, ~493줄)

대상:
- tests/spark_job/test_clickhouse_settings.py
- tests/spark_job/test_fact_settings.py
- tests/spark_job/test_timestamp_columns.py

현황: 수제 check()/PASS/FAIL/main() 집계 harness. 저장소 내
tests/simulator/test_api_simulator.py는 이미 pytest assert + parametrize
방식이라 참고 가능.

검토 요청:
- 전환 범위·순서
- check() 호출부를 assert로 매핑하는 방식
- 전환 중 놓치기 쉬운 커버리지 누락 지점

## 2. yagni Spark env 자동전환 계층 삭제

대상:
- scripts/autoswitch_spark_env.sh (123줄)
- scripts/apply_spark_env.sh (22줄)
- scripts/current_spark_env.sh (37줄)
- config/env/{low,mid,high}.env

현황: 세 스크립트 모두 존재하지 않는 경로(/home/kang/log-etlm) 하드코딩돼
있어 현재 실행 자체 불가. PoC는 단일 프로파일 고정 운용 중.

검토 요청:
- docker-compose 기본값으로 값 흡수하고 스크립트 3개 삭제가 맞는지
- config/env/ 디렉터리 자체를 없앨지, 참고용으로 남길지 판단

## 공통
검토 결과는 say lead로 보고.
