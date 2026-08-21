# 우선순위 4·5번 구현 지시 (architect 설계 승인 완료)

설계 근거: docs/architect-review/04_진단스크립트_통합_및_assemble_pipeline_파라미터_제거.md

## 4. 진단 스크립트 3종 → scripts/diag.sh 통합

- scripts/diag.sh <section> 신규 작성. section: ch(=clickhouse_diagnostics 5쿼리) / latency(=publish_spike_diag) / parts(=diag_partition_and_partlog) / all.
- 기존 3개 스크립트(clickhouse_diagnostics.sh, publish_spike_diag.sh, diag_partition_and_partlog.sh) 삭제.
- 의도적으로 버리는 기능: publish_spike_diag의 2번(sink/e2e p95)·3번(kafka→spark ingest p95) 쿼리 — 1번(segmented p95)이 같은 윈도우·GROUP BY로 뽑는 컬럼의 부분집합이라 새 정보 없음. 약 28줄 삭제.
- 함께 수정: clickhouse_diagnostics·diag_partition_and_partlog가 `docker exec -it` 써서 TTY 없는 환경(cron 등)에서 실패 → `-i`로 통일.
- diag_partition_and_partlog의 하드코딩된 log_user/log_pwd를 쿼리 줄에서 빼고 공통 환경변수로 정렬(기본값은 동일 유지).
- WINDOW_MIN은 latency 섹션 전용 유지, parts의 5분 윈도우는 성격 다르므로 상수로 유지.
- README.md 190-191행: 옛 스크립트명 → diag.sh 사용법으로 교체.
- crontab/watchdog/Grafana 참조 없음 확인됨 — 추가 확인 불필요.
- 커밋 분리(참고, 실제 커밋은 lead): refactor(scripts) diag.sh 추가+3종 삭제 / docs README.

## 5. assemble_pipeline() 등 주입 파라미터 제거

대상 (11개 파라미터, 9개 아님):
- log_simulator/pipeline_builder.py:30-45 — assemble_pipeline() 9개
- log_simulator/simulator/task_builder.py:26-34 — create_simulator_tasks
- log_simulator/simulator/task_builder.py의 create_service_tasks_only() — settings·queue_config 2개 (요청 문서에 없던 추가 발견분, 같은 패턴이라 함께 정리)
- log_simulator/publisher/worker_pipeline.py:89-101

전환 방식: "넘긴 값 없으면 설정 읽기" → "항상 설정 직접 읽기"로 통일.
- pipeline_builder: 키워드 파라미터 9개 삭제 + import 5개(SimulatorSettings/QueueThrottleConfig/PublisherSettings/DlqPublisher/KafkaProducerClient) 동반 삭제. typing.Any는 유지.
- task_builder: 두 함수에서 resolved_* 중간변수 제거.
- worker_pipeline: 내부에서 get_publisher_settings/get_client/get_dlq_publisher 직접 호출.

**주의 — dlq_publisher는 삭제 대상 아님**: spark 쪽 DLQ 파이프라인은 이미 삭제됐지만(commit 83a33e2), log_simulator/publisher/dlq.py(85줄)는 살아있고 worker_pipeline.py에서 계속 쓰인다. 이번 작업은 파라미터 주입 구조만 제거하고, dlq_publisher를 얻는 동작(get_dlq_publisher(resolved_producer) 호출) 자체는 그대로 유지할 것. dlq.py 삭제하지 말 것 — 별건.

호출부 영향 확인됨: engine.start()/engine.set_eps() 모두 필수 인자만 넘겨서 영향 없음. test_api_simulator.py는 engine 객체 자체를 patch해서 무관.

커밋: 세 파일이 한 시그니처 변경으로 묶여 있어 쪼개면 중간 커밋이 깨짐 — refactor(simulator) 한 건으로.

## 공통
- 각 항목 완료 후 관련 테스트 돌려서 통과 확인.
- 완료되면 say lead로 보고.
