# 11. `pipeline_settings_review_2026-11-14.md` 검증

검증일: 2026-11-15. 대상: 저장소 루트의 `pipeline_settings_review_2026-11-14.md`(8절 포함).
방법: 문서의 주장을 현재 워킹트리의 코드·설정과 대조했다. Docker 런타임은 확인하지 않았다. 문서와 코드는 수정하지 않았다.

## 결론

설정값·코드 경로에 대한 사실 주장은 대부분 맞다. 계산(22,000건, 5,500/s, 618초, 14.5 EPS, mem_limit 합 13.90GiB)도 모두 재현된다.
다만 아래 세 가지는 고쳐야 문서를 작업 기준으로 쓸 수 있다.

1. ClickHouse async insert 판단은 실효 여부가 확인되지 않은 전제 위에 서 있다 (A1).
2. 8절 지표 해석 중 하나는 과장이고(B1), 둘은 실제보다 약하게 쓰였다(B2, B3).
3. "권장 순서"가 8절 결론과 요약표를 반영하지 않는다 (D).

## A. 틀렸거나 근거가 흔들리는 판단

### A1. async insert가 실제로 동작하는지 미확인인데 최우선 위험으로 판정했다

- 문서는 `zz-log_user-async-profile.xml`의 `async_insert=1, wait_for_async_insert=0`을 근거로 "적재 완료 확인에 부적합"이라 판정하고, 권장 순서 2·5번에 올렸다.
- 같은 문서가 "실행 결과에 `system.asynchronous_insert_log` 테이블이 없다"고 적었다. ClickHouse 시스템 로그 테이블은 첫 기록 때 생성된다. 테이블이 없다는 것은 async insert가 한 번도 기록되지 않았을 가능성을 뜻한다 (추론, 런타임 미확인).
- async가 실효가 아니라면 6절의 busy timeout·buffer·`wait=1` 논의와 "동기 vs async+wait 비교"는 대상이 없다. Spark는 이미 동기 INSERT 응답을 받고 있는 것이 된다.
- 조치: 권장 순서 1번에서 `getSetting('async_insert')`(log_user 세션), `system.query_log`의 INSERT `written_rows`, `asynchronous_insert_log` 존재 여부를 먼저 확인하고, 그 결과로 6절 판정을 확정한다.

### A2. "파싱 실패 행을 걸러낸다"는 전제가 검증되지 않았다 (5절)

- `fact/parsers/event_log.py:15`의 필터는 `json IS NOT NULL`이다. Spark 3 이후 `from_json`은 깨진 JSON에 NULL이 아니라 필드가 모두 NULL인 struct를 돌려주는 것으로 알고 있다 (Spark 4.0.1에서 실측 필요).
- 그 경우 깨진 레코드는 버려지지 않고 `normalize_event.py`의 기본값으로 `service='unknown'`, `status_code=0`, `event_id=topic-partition-offset` 행이 되어 적재된다. 필터에 걸리는 것은 value가 NULL인 레코드뿐이다.
- 조치: "파싱 제외 건수 집계"를 설계하기 전에 깨진 JSON 한 건으로 동작을 확인한다. 결과에 따라 대사 항목이 "제외 수량"이 아니라 "`service='unknown'` 행 수"로 바뀐다.

### A3. `kafka_received_at`은 브로커 수신 시각이 아니다 (8-1 3행)

- `parse_event.py:17`은 Kafka 레코드 `timestamp`를 그대로 쓴다. 토픽에 `message.timestamp.type` 지정이 없으므로 기본값 CreateTime, 즉 프로듀서가 `produce()`를 호출한 시각이다.
- 따라서 문서가 제안한 명칭 "Kafka→저장"도 부정확하다. 현재 `e2e_ms`는 "produce() 호출→저장"이고 librdkafka 큐 대기·linger·전송을 포함한다. `producer_to_kafka`는 "이벤트 생성→produce() 호출"(애플리케이션 큐 대기)이다.

## B. 과장되거나 약하게 쓰인 판단

### B1. 과장 — "오류 집계가 도메인 실패를 포괄하지 않는다" (8-1 5행, 우선도 높음)

- `normalize_event.py:52-57`은 `status_code`가 없으면 `result='fail'`→500, `'success'`→200으로 채운다.
- 현재 모드 `SIM_EVENT_MODE=domain`에서는 `status_code >= 500`이 `result='fail'`과 같다. 지금은 차이가 없다.
- 차이는 `http`/`all` 모드에서만 생긴다 (order의 실패 코드 422/409는 5xx가 아니다). 우선도를 "모드 변경 시 조건부"로 낮춘다.

### B2. 약함 — 오류 복제가 오류율에 주는 영향 (8-1 4행)

- 문서는 "분자·분모가 함께 바뀌어 왜곡"이라고만 썼다. 실제로는 실패 이벤트가 전부 한 번 더 들어가므로 오류율이 약 2배가 된다 (auth: 실제 0.30% → 집계 약 0.60%).
- 같은 집계를 `infra/monitor/watchdog/clickhouse_probe.py`의 오류율 경보도 사용한다. 대시보드만이 아니라 경보 임계값도 영향을 받는다.

### B3. 약함 — Watchdog 범위 (8-4 5번)

- 문서는 Watchdog을 "컨테이너 감시"로 서술했다. 실제로는 `clickhouse_probe.py`가 EPS 하한, freshness, 단계별·E2E p95, 오류율 경보를 이미 갖고 있다.
- 추가할 것은 Kafka lag, 디스크 여유, async 실패 정도다. 단 compose에 monitor 서비스가 없으므로 실제 가동 여부는 확인 대상이다.

### B4. 약함 — 접근 제어 (8-3)

- 문서는 `29092`, `8123`만 언급했다. `8000, 8080, 8081, 7077, 4040, 5521, 3000`도 주소 제한 없이 게시된다.
- `kafka-ui`는 인증 없이 `READONLY=false`다. `ch-ui`는 `VITE_CLICKHOUSE_PASS`로 log_user 비밀번호를 브라우저에 전달한다.
- `access_management=1`은 `10_log_user.xml` 기준이다. 엔트리포인트가 만드는 `users.d/default-user.xml`(gitignore 대상, 로컬에 존재)이 알파벳 순서상 뒤에 로드되며 값이 0이다. 실효값은 0일 가능성이 높다 (런타임 미확인).

## C. 누락

- **검증 스크립트가 같은 결함을 물려받는다.** 새로 추가된 `scripts/validation_snapshot.sh`는 주기마다 `diag.sh all`을 호출한다. `diag.sh`는 `set -e` 상태에서 `diag_ch` 3번 쿼리(`asynchronous_insert_log`)가 실패하면 종료하므로, 스냅샷에 latency·EPS·parts 섹션이 한 번도 기록되지 않는다. lag(`kafka_spark_lag.py`)와 `docker stats`만 남는다.
- **`failOnDataLoss=false`는 설정이 아니라 코드 고정값이다** (`stream_kafka.py:33`). 3절의 producer 항목은 환경변수만 바꾸면 되지만 이 항목과 delivery 집계, 파싱 제외 집계는 코드 변경이다. 문서는 둘을 구분하지 않는다.
- **생성 EPS는 구조적으로 목표 이하이고 따라잡지 않는다.** `stream_pipeline.py:57-59`는 토큰을 `max_batch_size`(=1초치)로 자른다. 루프가 1초를 넘긴 만큼의 생성량은 버려진다. 문서는 soft throttle만 언급했다. "실제 생성 EPS 기록"의 근거로 추가할 내용이다.
- **기준선이 `.env`에 따라 달라진다.** `.env.example`은 `SPARK_CLICKHOUSE_JDBC_BATCHSIZE=10000`, `WRITE_PARTITIONS=3`이고 코드에 없는 변수(`SPARK_CLICKHOUSE_ALLOW_REPARTITION`)도 있다. 예시를 그대로 복사하면 문서의 15,000 기준과 다르다. `WRITE_PARTITIONS=3`은 pre_coalesce=2 뒤에서 효과가 없다.
- **관측 수치의 출처가 없다.** p95 1.337초, driver 91~98%, Kafka 71~85%, parts 7~18은 저장소에서 재현할 수 없고 원본 경로도 적혀 있지 않다. `validation_snapshot.sh` 산출물 경로를 근거로 붙여야 한다.
- **Kafka 중단 시 simulator OOM 경로가 명시되지 않았다.** `acks=0`에서는 평소 큐가 차지 않는다. 브로커가 멈추면 librdkafka 큐(20만 건/64MiB)가 찬 뒤 `publish_batch`가 BufferError 루프에 머물고, 애플리케이션 큐가 hard throttle(3,600개) 훨씬 전에 256MiB를 넘는다. 큐 축소의 실제 근거는 이 경로다.

## D. 권장 순서 평가

현재 순서의 문제:

- 요약표는 "관측 복구"를, 8절 결론은 "지표 정의와 파괴적 DDL"을 먼저라고 하는데 권장 순서 5개 항목에는 둘 다 없다. 8절을 추가한 뒤 순서를 갱신하지 않았다.
- 2번은 의미가 다른 변경 4개(체크포인트, producer 확인·집계, sink 응답, 파싱 집계)를 한 단계로 묶었다. 5번의 "한 조건씩" 원칙과 어긋나고, 설정 변경과 코드 변경이 섞여 있다.
- 큐 축소(4번)는 8-4의 Kafka 중단 시험보다 먼저여야 한다는 점이 드러나 있지 않다.

제안 순서:

1. 관측 복구: `diag.sh`가 없는 테이블에서 중단하지 않게 한다. 이것이 없으면 이후 모든 전후 비교가 불가능하다.
2. 실효 설정 수집: async insert 실효 여부(A1), 토픽 파티션·retention, log_user 실효 프로파일·권한, 깨진 JSON 동작(A2).
3. 지표 정의 정정: 오류 복제 분리(B2), `created/stored` 명칭, E2E 구간 명칭(A3). 이후 측정값의 의미가 여기서 정해진다.
4. 체크포인트 보존(`SPARK_RESET_CHECKPOINT_ON_START=false`): 설정만 바꾸며 처리량 영향이 없다. 단독으로 적용하고 재시작 복구를 확인한다.
5. sink 완료 응답: 2번 결과 async가 실효일 때만 진행한다.
6. producer `acks=all`·멱등성(설정) → delivery 집계 반영(코드).
7. 애플리케이션 큐 축소 → 그 뒤 Kafka 중단·Spark 강제 종료 시험.
8. driver 메모리, GC 고정값 등 자원 튜닝.

4초/22,000/파티션 2/batchsize 15,000/pool 4 유지 판단은 그대로 타당하다.

## 확인하지 않은 것

- Docker 런타임 상태 전부(실효 설정, 토픽, DDL, 메모리·lag 관측값).
- A1·A2·B4의 실효값은 코드와 파일에 근거한 추론이다.
- 문서가 인용한 외부 공식 문서 링크의 내용.
