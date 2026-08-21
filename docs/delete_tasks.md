# ponytail 감사 delete 태그 제거 작업 (docs/overengineering_audit.md 기반)

1. DLQ 파이프라인 전체: spark_job/dlq/, spark_job/clickhouse/sink.py:41-131의 DLQ 우회 로직, spark_job/stream_ingest.py:47-57 분기, infra/clickhouse/sql/10_fact.sql:47-68(event_log_dlq 테이블), infra/clickhouse/sql/21_materialized_views.sql:91-106(event_log_dlq_agg_1m MV), infra/monitor/watchdog/clickhouse_probe.py:221-227(_check_dlq_metric). docker-compose.yml의 DLQ 관련 env(SPARK_DLQ_*, SPARK_ENABLE_DLQ_STREAM, SPARK_CLICKHOUSE_DLQ_ON_FINAL_FAILURE)도 함께 정리.

2. scripts/capacity_calc.py 전체 삭제.

3. 배치 가드: spark_job/clickhouse/sink.py:134-216,232-295, spark_job/clickhouse/settings.py:35-36, infra/clickhouse/sql/10_fact.sql:71-80(stream_batch_guard 테이블). SPARK_CLICKHOUSE_BATCH_GUARD_ENABLED env도 정리.

4. 시간대 가중치: log_simulator/config/timeband.py, log_simulator/config/profile_context.py:20, log_simulator/simulator/stream_pipeline.py:70-72.

5. scripts/check_backlog.sh와 scripts/kafka_spark_lag.py 중 kafka_spark_lag.py(파이썬)만 남기고 check_backlog.sh 삭제.

6. spark_job/main.py:84-123의 _log_runtime_env/_load_env_keys_from_profile/_mask_env_value 삭제.

7. spark_job/clickhouse/sink.py:269-345 재시도 루프 삭제 — 예외를 올려 Spark 체크포인트 재처리에 위임.

8. 미사용 공개 API 6종 삭제: log_simulator/producer/kafka_client.py의 publish_batch_direct/get_producer/build_producer_config(+producer/__init__.py 재export), log_simulator/publisher/dlq.py:83-85의 publish_dlq_batch, log_simulator/simulator/stream_helpers.py:32-51의 build_batch_messages, log_simulator/simulator/base.py:405-410의 BaseServiceSimulator.generate_events(count).

9. log_simulator/engine.py:116-120,158-164의 run_simulator()/SimulatorEngine.wait() 및 관련 self._tasks 삭제.

10. log_simulator/simulator/base.py:57-58,137-138,274-304의 domain_event_rate/http_event_rate 필드·추정 메서드·slots 삭제.

11. spark_job/fact/schema.py:30-34, spark_job/fact/transforms/normalize_event.py:14-70의 레거시 필드(timestamp_ms/level/path/product_id) 및 coalesce 폴백, infra/clickhouse/sql/10_fact.sql의 대응 컬럼 삭제.

12. raw_json 저장 경로 전체: spark_job/fact/transforms/parse_event.py:8-30, spark_job/fact/parsers/event_log.py, spark_job/fact/transforms/normalize_event.py:79의 store_raw_json 파라미터 체인·컬럼 삭제. docker-compose SPARK_STORE_RAW_JSON env도 정리.

13. spark_job/clickhouse/settings.py:59-97,108의 build_native_options()/build_catalog_configs()/jdbc_fetchsize 삭제. README.md:182 언급도 함께 정리.

14. tests/simulator/test_render_bytes.py 전체 삭제.

15. infra/monitor/watchdog/docker_probes.py:12-23,33,61의 stream_lines() 반환값에서 proc 제거, 반환 타입을 스트림 하나로 축소.

16. infra/clickhouse/migrations/01_drop_redundant_mvs.sql 삭제(이미 적용 완료된 일회성 마이그레이션).

## 공통 지시
- 각 항목 제거 후 참조 끊긴 곳(다른 파일의 import·호출·SQL 참조) 전부 정리.
- 끝나면 관련 테스트·lint 돌려서 통과 확인.
- yagni/shrink/stdlib/native 태그 항목은 이번 지시에서 제외 — 별도 지시 예정.
- 완료되면 say lead로 보고.
