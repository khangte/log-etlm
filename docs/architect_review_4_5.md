# 우선순위 4·5번 설계 검토 요청 (docs/overengineering_audit.md 미처리 항목)

## 4. shrink 진단 스크립트 3종 통합 (audit line 45)

대상 (scripts/):
- clickhouse_diagnostics.sh (66줄)
- publish_spike_diag.sh (88줄)
- diag_partition_and_partlog.sh (24줄)

현황: 셋 다 `docker exec clickhouse clickhouse-client` 접속 상용구가 겹치고,
지표 쿼리도 일부 중복.

검토 요청:
- 하나로 합치고 섹션을 인자로 고르는 구조 설계(예: `diag.sh <section>`)
- 각 스크립트의 고유 쿼리/기능 손실 없는지 확인
- 기존 호출부(문서·crontab·watchdog 등)에서 개별 스크립트명을 참조하는 곳 있는지

## 5. yagni assemble_pipeline() 주입 파라미터 9개 제거 (audit line 22)

대상:
- log_simulator/pipeline_builder.py:30-45
- log_simulator/simulator/task_builder.py:26-34
- log_simulator/publisher/worker_pipeline.py:89-101

현황: `assemble_pipeline()`의 파라미터 9개(simulator_settings, queue_config,
publisher_settings, producer, dlq_publisher, worker_count, log_batch_size,
queue_size, loops_per_service)를 유일 호출자 `engine.start()`가 전부 생략해
None으로 넘기고, 하위 함수가 다시 `or get_*()`로 되돌리는 구조.

주의: dlq_publisher 파라미터는 이번 세션에서 DLQ 파이프라인 전체가 이미
삭제됐으므로(우선순위 1번 delete 작업, commit 83a33e2) 현재 코드에 남아
있는지부터 확인 — 이미 제거됐을 수 있음.

검토 요청:
- 파라미터 제거 후 하위에서 설정 직접 읽는 구조로 전환 설계
- 호출부(engine.start()) 영향 범위
- 테스트 코드에서 이 파라미터들을 목/주입하는 곳이 있는지(있다면 함께 정리 필요)

## 공통
검토 결과는 say lead로 보고.
