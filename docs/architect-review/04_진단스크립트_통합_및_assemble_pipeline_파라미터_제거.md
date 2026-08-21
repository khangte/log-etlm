# 우선순위 4·5번 설계 검토 결과

- 요청: lead (docs/architect_review_4_5.md)
- 검토일: 2026-08-21
- 대상: docs/overengineering_audit.md line 45(진단 스크립트 3종), line 22(assemble_pipeline 주입 파라미터)

---

## 4. 진단 스크립트 3종 통합

### 결론

`scripts/diag.sh <section>` 하나로 합친다. 178줄(66+88+24) → 약 110줄.

섹션은 기존 스크립트와 1:1로 대응시켜 재편성하지 않는다. 그래야 기존 사용자가
"이 스크립트가 하던 일"을 그대로 찾을 수 있고 리뷰도 쉬워진다.

| 섹션 | 대체 대상 | 내용 |
| --- | --- | --- |
| `ch` | clickhouse_diagnostics.sh | merges, active parts, async insert log, query_log INSERT, 현재 async 설정 (5쿼리) |
| `latency` | publish_spike_diag.sh | 구간별 p95, 분당 eps, publish_p95 상위 서비스 (3쿼리) |
| `parts` | diag_partition_and_partlog.sh | spark-driver sink 로그 tail, part_log NewPart 분포 |
| `all` | — | 세 섹션 순차 실행 |

공통부는 환경변수 기본값 블록(`CLICKHOUSE_CONTAINER`, `CLICKHOUSE_USER`,
`CLICKHOUSE_PASSWORD`, `CLICKHOUSE_DB`, `WINDOW_MIN`), `run_query()`,
섹션 제목 출력 헬퍼, `usage()` 정도면 충분하다.

### 기능 손실 검토

**손실 없음.** 다만 의도적으로 버리는 것이 두 개 있다.

`publish_spike_diag.sh`의 2번(`sink/e2e p95`)과 3번(`kafka->spark ingest p95`)
쿼리는 1번(`segmented p95`)이 이미 같은 윈도우·같은 GROUP BY로 뽑는 다섯 컬럼의
부분집합을 다시 조회하는 것이다. 같은 테이블에 같은 조건이라 새로 얻는 정보가
없다. 이 두 쿼리를 지운다(약 28줄). 감사가 말한 "겹치는 지표 쿼리"가 바로 이것이다.

그 외 쿼리는 모두 고유하므로 전부 옮긴다.

### 통합하면서 같이 고쳐야 할 것

- `clickhouse_diagnostics.sh`와 `diag_partition_and_partlog.sh`는 `docker exec -it`을
  쓴다. TTY가 없는 환경(cron, CI, 파이프)에서 `the input device is not a TTY`로
  실패한다. `publish_spike_diag.sh`처럼 `-i`로 통일한다.
- `diag_partition_and_partlog.sh`만 `#!/bin/bash`이고 접속 정보(`log_user` / `log_pwd`)를
  쿼리 줄에 직접 박아 두었다. `#!/usr/bin/env bash` + 공통 환경변수로 맞춘다.
  기본값은 기존과 동일하게 두어 동작 변화를 만들지 않되, `CLICKHOUSE_PASSWORD`가
  export되어 있으면 그것을 우선 쓰는 현재 두 스크립트의 방식을 표준으로 삼는다.
- `WINDOW_MIN`은 `latency` 섹션에만 의미가 있으므로 기본값 10을 유지하고,
  `parts` 섹션의 5분 윈도우는 그대로 상수로 둔다(성격이 다른 값이다).

### 호출부

- `README.md` 190-191행이 세 스크립트를 이름으로 안내한다 → `scripts/diag.sh` 한 줄로 교체.
- `crontab -l`은 비어 있고, watchdog(`infra/monitor`)과 Grafana는 이 스크립트들을
  참조하지 않는다. 그 외 참조부는 감사 문서와 이번 검토 요청 문서뿐이다.

### 커밋 분리

- `refactor(scripts)`: `scripts/diag.sh` 추가 + 기존 3종 삭제
- `docs`: README.md

---

## 5. assemble_pipeline() 주입 파라미터 제거

### 결론

제거한다. 9개가 아니라 **11개**다.

`docs/architect_review_4_5.md`가 언급하지 않은 `create_service_tasks_only()`의
`settings`, `queue_config` 두 파라미터도 같은 패턴(호출자가 생략 → 내부에서
`or get_*()`로 복원)이고 같은 파일에 있다. 함께 정리한다.

### 선행 확인 — dlq_publisher는 아직 살아 있다

요청 문서는 커밋 `83a33e2`에서 DLQ가 삭제됐으니 파라미터가 이미 없을 수 있다고
했는데, **그 커밋은 spark_job과 infra만 건드렸다.** 시뮬레이터 쪽
`log_simulator/publisher/dlq.py`(85줄)는 그대로 있고
`worker_pipeline.py:14,35,57,93,98,110`에서 계속 쓰인다.

이 파일을 지울지는 별건이다. Spark 쪽과 달리 이 경로는 완전한 사문(死文)이 아니다.
`logs.dlq` 토픽 생성이 kafka-init에서 주석 처리돼 있긴 하나
`KAFKA_AUTO_CREATE_TOPICS_ENABLE=true`라 실제로 발행되면 토픽이 생성되고,
publish 예외 시에만 타는 경로라 도달 가능성이 0은 아니다.
**이번 작업에서는 파라미터만 없애고 동작은 그대로 둔다** —
`create_publisher_workers()` 내부에서 `get_dlq_publisher(resolved_producer)`를
계속 호출한다. 시뮬레이터 DLQ 경로 자체의 존폐는 별도 delete 판단으로 올린다.

또한 감사 문서 line 10(DLQ 항목)이 `[처리완료]`로 마킹돼 있는데
시뮬레이터 쪽이 남아 있으므로, 해당 줄에 잔여 범위를 명시하거나
별도 항목으로 분리해야 한다.

### 전환 설계

세 함수 모두 "호출자가 넘긴 값 없으면 설정을 읽는다"를 "항상 설정을 읽는다"로 바꾼다.

**log_simulator/pipeline_builder.py**

```python
def assemble_pipeline(
    simulators: dict[str, Any],
    base_eps: float,
    service_eps: dict[str, float],
) -> Pipeline:
```

키워드 인자 9개 전부 삭제. 하위 호출도 위치·이름 인자 없이 넘긴다.
이에 따라 `SimulatorSettings`, `QueueThrottleConfig`, `PublisherSettings`,
`DlqPublisher`, `KafkaProducerClient` import 5개가 함께 빠진다
(`typing.Any`는 `simulators` 타입에 계속 필요하므로 유지).

**log_simulator/simulator/task_builder.py**

- `create_simulator_tasks()`: `log_batch_size`, `queue_size`, `loops_per_service`,
  `settings`, `queue_config` 삭제. 본문 첫 줄에서
  `settings = get_simulator_settings()`, `queue_config = get_queue_config(settings)`를
  읽고 `resolved_*` 중간 변수 3개를 없앤다.
- `create_service_tasks_only()`: `settings`, `queue_config` 삭제. 동일 처리.

**log_simulator/publisher/worker_pipeline.py**

- `create_publisher_workers()`: `worker_count`, `settings`, `producer`,
  `dlq_publisher` 삭제. 내부에서 `get_publisher_settings()`, `get_client()`,
  `get_dlq_publisher(producer)`를 직접 호출한다.

### 호출부 영향

없다. 유일한 호출자인 `engine.start()`(`log_simulator/engine.py:52-55`)는
`simulators`, `base_eps`, `service_eps` 세 개만 넘기고,
`engine.set_eps()`(`engine.py:95-100`)도 `create_service_tasks_only()`에
`simulators`, `base_eps`, `service_eps`, `publish_queue`만 넘긴다.
선택 파라미터를 실제로 채워 넘기는 코드는 저장소 전체에 하나도 없다.

### 테스트 영향

없다. `tests/simulator/test_api_simulator.py`는 `log_simulator.api.simulator.engine`
객체 자체를 `patch`로 갈아끼우므로 이 파라미터들을 건드리지 않는다.
저장소의 다른 테스트도 마찬가지다.

### 감수하는 것

주입 지점이 사라지므로 가짜 producer를 넣어 퍼블리셔를 단위 테스트하는 seam이
없어진다. 다만 지금 그런 테스트가 없고, 필요해지면 `get_client`를
monkeypatch하면 된다. 쓰지 않는 주입 경로를 미리 열어 두는 것이 YAGNI다.

### 후속

파라미터를 걷어내면 `create_simulator_tasks()`와 `create_service_tasks_only()`는
"publish_queue를 새로 만드는가"만 다른 거의 같은 함수가 된다.
task_builder 중복 제거(감사 별도 항목)는 이 작업 **뒤에** 처리하는 편이 쉽다.

### 커밋 분리

`refactor(simulator)` 한 건. 세 파일이 하나의 시그니처 변경으로 묶여 있어
쪼개면 중간 커밋이 깨진다.
