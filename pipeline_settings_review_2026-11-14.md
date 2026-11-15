# 5,000 EPS 파이프라인 설정 검토

검토일: 2026-11-14. 대상: `/home/kang/projects/log-etlm`의 현재 파일 및 앞서 수집한 WSL 실행 기록. 코드·설정은 수정하지 않았다. 실무 운영 관점의 추가 검토는 8절에 기록했다. 추가 검토는 저장소와 공식 문서에 근거하며 런타임 재측정 결과는 아니다.

## 단계별 판단 요약

| 단계 | 현재 설정 | 판단 | 우선 행동 |
|---|---|---|---|
| 생성 주기 | 1초, 서비스별 루프 1개 | 유지 가능. 초당 묶음 입력 | 실제 생성 EPS 기록 |
| 생성 배치 | LOG_BATCH_SIZE=500 | 실제 500건 상한으로 작동하지 않음 | 서비스별 실제 배치 크기 기준으로 해석 |
| 애플리케이션 큐 | QUEUE_SIZE=4000, 컨테이너 256MiB | 배치 목록 4천 개로 과도한 용량 | 16~32개 목록을 비교 후보로 검토 |
| Publisher | 워커 3개, 배치 600 | 증설 근거 없음. 600은 엄격한 최대치 아님 | 유지하며 enqueue와 delivery 구분 |
| Producer 배치 | linger 20ms, 최대 5천 메시지 | 유지 가능 | 병목 발생 시에만 재검토 |
| Producer 전달 | acks=0, 멱등성 비활성 | 수신 확인이 없어 정합성 검증에 부적합 | acks=all과 멱등성 활성화를 비교 기준으로 사용 |
| Kafka 파티션 | auth/order/payment/error=3/2/1/1 | 부하 비중과 일관, 증설 근거 없음 | 유지하고 실제 토픽 설정 확인 |
| Kafka 메모리 | 컨테이너 2GiB, heap 최대 1.5GiB | 증설 근거 없음. 비힙 여유 확인 필요 | GC와 cgroup 사용량 관측 |
| Kafka 보관 | 1시간, 파티션당 20GiB | 정상 소비에는 가능. 장시간 복구에는 짧음 | 복구 목표와 디스크 예산으로 결정 |
| Spark 입력 | 4초, 자동 상한 22,000건 | 5천 EPS와 일관 | 유지 |
| Spark 병렬성 | 워커 4+2코어, coalesce 2 | 처리 가능. 6코어 활용 효율은 미검증 | 증설 없이 필요 시 2+2와 비교 |
| Spark driver | heap 768MiB, 컨테이너 1,500MiB | 관측 메모리 여유 부족 | JVM/Python/cgroup 사용량 구분 |
| Spark 복구 | 체크포인트 삭제, latest 시작 | 재시작 시 backlog를 건너뛸 수 있음 | 체크포인트 보존을 검증 기준으로 사용 |
| JDBC | 쓰기 파티션 2, batchsize 15,000 | 현재 부하 규모에 합리적 | 유지 |
| ClickHouse async | 파일 설정은 완료 대기 없음, timeout 5초. 실효 여부 미확인 | 실효라면 적재 완료 확인과 지연 검증에 부적합. 실효가 아니면 해당 없음 | 실효 여부를 먼저 확인한 뒤 동기 INSERT와 async+완료 대기 비교 |
| ClickHouse 메모리 | 컨테이너 5GiB, query/user 제한 6GiB | 내부 제한이 컨테이너보다 큼 | 기존 오류 재현 후 제한 관계 재설계 |
| ClickHouse 병합 | background pool 4 | 관측 parts 적체 없음 | 유지 |
| 관측 | 진단 중 쿼리 실패로 후속 수집 중단. 검증 스냅샷 스크립트도 같은 경로 사용 | 최적화 판단에 필요한 지표 누락 | `diag.sh` 중단을 가장 먼저 고쳐 적재 EPS·E2E·오류 수집 복구 |
| 지표 정의 | 오류 복제를 포함해 집계, 지연 구간 명칭이 실제 시각과 불일치 | 오류율이 약 2배로 집계됨. E2E 명칭 부정확 | 측정 전에 복제 분리와 명칭 정정 |

표의 변경 후보는 확정 최적값이 아니다. 아래 절에서 코드 근거, 계산, 검증 조건을 설명한다.

## 판정 범위

- 현재 compose와 코드의 설정 전달 경로를 추적했다. 관련 `.env` 및 현재 셸에는 compose가 치환하는 Spark 튜닝 변수의 별도 값이 없었다. 비밀값은 출력하지 않았다.
- 현재 Docker에 접근할 수 없어 실행 중 유효 설정, 기존 토픽 설정, ClickHouse에 실제 적용된 프로파일과 DDL은 미확인이다. 특히 async insert 로그 XML이 파일에 있는데 실행 결과에는 해당 테이블이 없어 설정 파일과 실행 상태를 동일시하면 안 된다.
- 최근 관측 구간은 약 2시간, 배치 기록 전체는 약 4시간 9분이다. 24회 표본의 lag 및 메모리는 연속 계측이 아니다.
- '최적'의 기준은 처리량 외에 허용 지연, 장애 시 누락·중복 허용 범위, 자원 예산에 따라 달라진다. 아래 변경 후보는 검증할 기준값이며 최적값으로 단정하지 않는다.

## 종합 판단

4초 트리거, 자동 입력 상한 22,000건, 쓰기 파티션 2개, JDBC batchsize 15,000은 5천 EPS의 부하 규모와 일관된다. 관측 구간에서도 두 계측 단계 합산 p95 1.337초와 4초 초과 0건이 확인된다. 처리량 증설이 필요한 근거는 없다.

반면 수신 확인 없는 producer와 재시작 시 체크포인트 삭제는 오류·미처리 데이터를 감춘 채 빠르게 보일 수 있는 조합이다. 파일 설정상 적재 완료를 기다리지 않는 ClickHouse 응답도 같은 위험이지만, 이 설정이 실제로 적용되는지는 미확인이다(6절). 성능 수치의 기준을 만들려면 관측 경로와 지표 정의를 먼저 바로잡고 이 경로를 정리해야 한다. 큐 단위와 실제 배치 크기, 메모리 제한 간 불일치도 존재한다.

## 1. 실제 부하 계산

근거: `log_simulator/config/profiles.yml`, `simulator/eps_policy.py`, `simulator/stream_helpers.py`, `producer/kafka_client.py`.

| 대상 | 목표 또는 기대 EPS | 파티션 | 파티션당 평균 EPS |
|---|---:|---:|---:|
| auth | 2,500 | 3 | 약 833 |
| order | 1,500 | 2 | 750 |
| payment | 1,000 | 1 | 1,000 |
| error 복제 | 약 14.5 | 1 | 약 14.5 |
| Kafka 전체 | 약 5,014.5 | 7 | — |

`logs.error`는 실패 이벤트를 원래 서비스 토픽에 넣은 뒤 추가 발행한 복제다. 오류율 가중합은 `2500×0.003 + 1500×0.002 + 1000×0.004 = 14.5`이다. Spark가 네 토픽을 모두 구독하므로 원본 5천 건 외에 오류 복제도 처리한다. 실제 오프셋 전진량 약 5,014/s와 부합한다. `logs.error`는 파싱 실패 DLQ가 아니다.

정합성 검증 시 원본 이벤트와 복제 레코드를 구분해야 한다. Kafka→ClickHouse 전달 검증은 `(topic, kafka_partition, kafka_offset)` 기준이 적절하다. `event_id` 중복만으로 재처리 중복이라고 판정하면 의도된 오류 복제를 잘못 셀 수 있다.

## 2. 시뮬레이터와 publisher

근거: `docker-compose.yml:14`, `log_simulator/simulator/stream_pipeline.py:37`, `log_simulator/pipeline_builder.py`, `log_simulator/publisher/worker_helpers.py`.

- `TARGET_INTERVAL_SEC=1`, `LOOPS_PER_SERVICE=1`, `SIMULATOR_SHARE=1`: 5천 EPS 생성 목표와 일관된다. 다만 서비스별 약 1초 단위의 묶음 입력이며 균일하게 매 0.2ms마다 한 건을 보내는 부하는 아니다.
- `LOG_BATCH_SIZE=500`: 현재 코드에서 엄격한 배치 크기가 아니다. 생성 상한은 `max(500, ceil(서비스 EPS×1초))`이므로 서비스별 최대 약 2,500/1,500/1,000건이다. 입력 평탄화가 목표라면 간격 단축과 실제 배치 크기 검증을 함께 해야 한다. 현 관측에서 처리량 병목 근거가 없어 우선 유지 가능하다.
- `WORKER_BATCH_SIZE=600`: `collect_batch`는 가져온 목록을 잘라내지 않고 통째로 합친다. 첫 목록이 2,500건이면 그대로 발행한다. 600은 엄격한 최대치가 아니다.
- `PUBLISHER_WORKERS=3`: 단일 event loop의 asyncio 태스크이며 하나의 producer를 공유한다. 워커 수와 600을 곱해 초당 최대 처리량을 계산할 수 없다. 워커 수 증설 근거는 없다.
- `QUEUE_SIZE=4000`: 건수가 아니라 `list[BatchMessage]` 4천 개다. soft throttle은 3,400개, hard throttle은 3,600개에서 작동한다. 현재 서비스별 생성 묶음을 고려하면 수백만 이벤트가 들어갈 수 있어 256MiB 컨테이너의 메모리 안전장치로 부적절하다.
- 큐 변경 후보: 16~32개 목록을 초기 비교 대상으로 삼고 실제 큐 점유·발행 지연·메모리로 결정한다. 평균 목록 약 1,667건을 가정하면 약 2.7만~5.3만 이벤트로, 5천 EPS에서 약 5~11초 분량이다. 목록 크기가 달라지므로 이는 용량 보장이 아니다.
- producer 내부 큐 상한은 별도로 64MiB/20만 메시지다. 애플리케이션 큐 메모리와 합산해 봐야 한다.
- soft throttle은 생성 목표를 최대 20%까지 낮춘다. 설정 EPS가 5천이어도 부하가 밀리면 실제 생성량은 줄 수 있으므로 생성·enqueue·delivery 완료 건수를 구분해 기록해야 한다.

## 3. Kafka producer와 broker

근거: `docker-compose.yml:44`, `docker-compose.yml:74`, `docker-compose.yml:138`, `log_simulator/producer/settings.py`, `producer/kafka_client.py`.

- `acks=0`, `enable.idempotence=false`: 속도 우선 선택이다. 브로커 수신 여부를 확인할 수 없어 정합성 검증 기준으로 부적합하다. 비교 기준은 `acks=all`, `enable.idempotence=true`를 권장한다. 단일 브로커/RF=1에서는 복제 내구성까지 생기지 않는다. producer 멱등성도 파이프라인 전체 exactly-once를 보장하지 않는다.
- `publish_batch` 성공은 비동기 enqueue 완료다. delivery callback의 오류는 경고만 남기고 publisher의 성공 통계에 되돌리지 않는다. 설정 변경만으로 생성→Kafka 성공 집계가 정확해지는 것은 아니다.
- `linger.ms=20`, `batch.num.messages=5000`: 유지 가능하다. 5,000개는 배치를 채워야 전송하는 최소치가 아니다. 묶음 입력 패턴과 파티션별 배치 형성을 함께 고려해야 한다.
- `compression=none`: 로컬 전송에서 반드시 나쁜 값은 아니다. 디스크·전송 비용을 줄이려면 `lz4`를 동일 조건으로 비교하되 CPU와 지연을 함께 측정한다. Spark→ClickHouse 압축과 독립된 설정이다.
- auth/order/payment의 3/2/1 파티션은 부하 비중과 대체로 일관되고 병목 증거가 없다. 추가 파티션 불필요. `KAFKA_NUM_PARTITIONS=1`은 새 자동 생성 토픽의 기본값이며 명시적으로 생성한 토픽의 값과 모순되지 않는다.
- init의 `--create --if-not-exists`는 기존 토픽의 파티션 수를 변경하지 않는다. 실제 파티션과 retention override는 실행 시 확인해야 한다.
- Kafka 컨테이너 2GiB, heap 최대 1.5GiB: JVM 비힙과 프로세스 비용 여유는 약 0.5GiB다. 관측 사용률 71~85%로 당장 heap을 늘릴 근거가 없다. 압박 발생 시 heap 1GiB 비교가 후보이며 GC/lag 악화 여부를 확인한다.
- retention 1시간: 소비 지연이 작은 현재 부하에는 가능하나 장시간 중단 후 재처리 목적에는 짧다. 복구 허용 시간에 맞춰 결정한다. 6~24시간은 디스크 용량을 확인한 뒤 비교할 후보이지 필수값이 아니다.
- `log.retention.bytes=20GiB`는 파티션별 한도다. 7파티션이면 설정상 약 140GiB 규모이며 전체 디스크 20GiB 제한이 아니다. 시간 제한과 세그먼트 단위 삭제도 함께 작동한다.
- segment 512MiB와 retention 검사 10분 때문에 설정 1시간에 정확히 맞춰 모든 레코드가 지워지지는 않는다. 크기가 작은 error 토픽은 roll 시간도 확인해야 한다.

## 4. Spark 입력·병렬성·메모리

근거: `docker-compose.yml:202`, `spark_job/stream_kafka.py`, `stream_checkpoint.py`, `spark.py`, `clickhouse/writer_base.py`, `infra/spark/spark-defaults.conf`.

- 입력 상한은 `5000×4×1.1=22000`이며 cap 40,000은 현재 비활성 상한이다. 4초마다 상한을 처리할 수 있다는 가정의 오프셋 예산은 5,500/s로 오류 복제 포함 기대 입력 대비 약 485.5/s 여유다. 실제 처리 능력을 보장하는 공식은 아니다.
- 예를 들어 backlog 30만 건은 위 조건에서 회수에 약 618초가 필요하다. 정상 부하 추종에는 맞지만 대규모 backlog를 빠르게 지우는 설정은 아니다. 그렇다고 측정 없이 상한을 올릴 근거도 없다.
- `pre_coalesce=2`, `write_partitions=2`: 의도상 정합적이다. 앞에서 이미 2개로 줄였다면 뒤의 coalesce는 추가 축소를 하지 않는다. 입력 총 약 2만 건/배치에서 평균 1만 건 정도를 두 쓰기 태스크가 나누는 구조다. 실제 분포는 균등 보장되지 않는다.
- 워커 코어는 4+2지만 coalesce 이후 단일 stage에서 쓰기 태스크 2개가 6코어를 모두 활용하지는 못한다. 코어가 많다고 처리량이 비례 증가하지 않는다. 현 값을 유지하며 필요하면 2+2와 비교해 자원 절감 여부를 확인한다. 워커 삭제는 현재 근거로 권장하지 않는다.
- executor heap 1GiB / worker 등록 메모리 1.5GiB / worker 컨테이너 2GiB는 서로 다른 범위다. worker 데몬, executor 비힙 등을 실제 cgroup 사용량으로 봐야 한다.
- Standalone에서 `spark.executor.memoryOverhead=384m`와 `spark.driver.memoryOverhead=256m`는 YARN/Kubernetes의 메모리 할당·보호 장치처럼 적용되지 않는다. Docker 한도까지 자동 합산·예약한다고 해석하면 안 된다.
- driver heap 요청 768MiB, 컨테이너 1,500MiB에서 사용률 91~98%가 관측되어 여유가 작다. heap 증설보다 JVM/Python/RSS/cgroup 및 이벤트 보존량을 먼저 구분한다. 필요 시 컨테이너 2GiB를 비교하되 전체 Docker 메모리 예산 안에서 수행한다.
- 이 저장소는 `python -m` 진입이며 PySpark가 최초 gateway 생성 시 설정을 spark-submit에 전달하는 경로다. builder에 driver.memory를 썼다는 이유만으로 768MiB가 무시된다고 단정할 수 없다. 실제 JVM `-Xmx` 확인이 필요하다.
- 1GiB heap에 `G1HeapRegionSize=16m`은 약 64개 region이므로 비교 근거가 없는 세부 튜닝이다. 자동 선택과 비교할 후보이며 로그 없이 성능 악화를 확정하지 않는다.
- `skip_empty=true`는 모든 배치에 persist/isEmpty를 추가한다. 빈 배치가 드문 지속 부하에서 항상 유리하다고 단정할 수 없다. 현재 처리 시간이 짧아 우선순위는 낮다.

## 5. Spark 재시작 및 누락·중복

- `restart: unless-stopped` + `SPARK_RESET_CHECKPOINT_ON_START=true` + `startingOffsets=latest`: 오류로 재시작하면 체크포인트를 지우고 최신부터 읽으므로 기존 backlog를 건너뛸 수 있다. `stream_checkpoint.py`는 실제로 디렉터리를 삭제한다.
- 지속 실행·복구 검증 기준으로 `RESET_CHECKPOINT_ON_START=false`를 권장한다. `startingOffsets`는 기존 체크포인트가 있으면 복구 위치를 덮어쓰지 않는다. 새 검증을 시작할 때만 시작 오프셋을 명시적으로 결정한다.
- Kafka reader의 `failOnDataLoss=false`는 retention 등으로 오프셋이 없어질 때 실패 대신 진행할 수 있어 정합성 문제를 놓칠 수 있다. 검증 목적이라면 true를 후보로 삼는다.
- 현재 실제 Spark writer에는 batch guard 로직이 없다. 근거 문서의 가드 설명이나 예전 환경변수만 믿고 켜졌다고 판단하면 안 된다. 가드 옵션 추가만으로 멱등성이 생기지 않는다.
- `MergeTree`는 중복 제거를 보장하지 않고, JDBC foreachBatch는 부분 성공 후 재시도 시 중복될 수 있다. 체크포인트 보존과 적재 완료 응답을 적용해도 exactly-once는 아니다.
- 파서는 `json IS NULL` 행만 걸러내며(`fact/parsers/event_log.py:15`), 현재 Spark 코드에는 해당 행을 별도 DLQ에 저장하는 경로가 없다. 이 필터가 깨진 JSON을 제외하는지는 미확인이다. Spark 3 이후 `from_json`은 깨진 JSON에 NULL이 아니라 필드가 모두 NULL인 struct를 반환하는 것으로 알려져 있어, 필터에 걸리는 것은 value가 NULL인 레코드뿐일 수 있다(추론, Spark 4.0.1 실측 필요).
- 그 경우 깨진 레코드는 버려지지 않고 `normalize_event.py`의 기본값으로 `service='unknown'`, `status_code=0`, `event_id=topic-partition-offset` 행이 되어 적재된다. 깨진 JSON 한 건으로 동작을 확인한 뒤 누락 검증 항목을 정한다. 결과에 따라 대사 대상은 "필터 제외 수량" 또는 "`service='unknown'` 행 수"가 된다.

## 6. ClickHouse 쓰기와 자원

근거: `docker-compose.yml:236`, `spark_job/clickhouse/settings.py`, `sink.py`, `infra/clickhouse/users.d/zz-log_user-async-profile.xml`, `config.d/60-memory.xml`, `config.d/50-background-pool.xml`, `sql/10_fact.sql`, `sql/21_materialized_views.sql`.

- JDBC batchsize 15,000, 쓰기 파티션 2개: 약 2만 건을 쓰는 일반 배치에서는 태스크당 약 1만 건으로, batchsize 15,000을 채우지 않아도 파티션 끝에서 전송한다. 유지 가능하며 1만과 1.5만의 큰 차이를 가정하지 않는다. 한쪽 파티션에 데이터가 몰리면 여러 요청이 생길 수 있다.
- 기대 요청 수는 균등 분포 가정에서 약 2건/4초다. 이미 충분히 묶어 쓰므로 초소형 INSERT를 합치기 위한 추가 async buffer가 꼭 필요한지는 비교할 가치가 있다.
- **async insert의 실효 여부는 미확인이다.** `async_insert=1`, `wait_for_async_insert=0`은 `zz-log_user-async-profile.xml`의 파일 설정이다. 실행 결과에는 `system.asynchronous_insert_log` 테이블이 없었다. ClickHouse 시스템 로그 테이블은 첫 기록 때 생성되므로, 테이블이 없다는 것은 async insert가 한 번도 기록되지 않았을 가능성을 뜻한다(추론, 런타임 미확인).
- 따라서 아래 async 관련 항목은 모두 "async가 실효일 때"의 조건부 판단이다. 실효가 아니라면 Spark는 이미 동기 INSERT 응답을 받고 있으며 busy timeout·buffer·`wait=1` 논의와 동기/async 비교는 대상이 없다. 먼저 log_user 세션의 `getSetting('async_insert')`, `system.query_log`의 INSERT `written_rows`, `asynchronous_insert_log` 존재 여부를 확인한다.
- (실효일 때) `async_insert=1`, `wait_for_async_insert=0`: 쓰기 성공이 flush 완료가 아니다. flush 실패가 Spark에 전달되지 않아 체크포인트만 전진할 수 있다. 정합성 검증에서는 `wait_for_async_insert=1`을 우선 권장한다.
- 대안은 현재 약 1만 건 단위의 배치를 유지하며 `async_insert=0`으로 동기 INSERT하는 것이다. async+wait=1과 하나씩 비교해 전체 trigger 시간·실제 적재 EPS·parts·지연으로 선택한다.
- busy timeout 설정 5초, buffer 64MiB는 시간 목표 및 실제 insert 크기와 함께 평가해야 한다. 5초를 고정 flush 주기로 단정할 수 없으며 adaptive 설정과 유효 timeout을 확인해야 한다. wait=1만 켜고 나머지를 그대로 두면 flush 대기로 4초 트리거를 넘을 수 있다. 비교 실험은 실제 timeout을 확인하고 예컨대 1초 수준을 후보로 잡는다.
- query/user 메모리 제한 6GiB가 컨테이너 5GiB보다 크고 서버 한도는 비활성이다. 내부 제한이 컨테이너 종료 전에 보호하는 구조가 아니다. 측정된 ClickHouse 사용량 약 1~1.7GiB만으로 5GiB를 줄이거나 6GiB 내부 상한을 최적이라고 볼 수 없다.
- 서버 한도 비활성에는 기존 문제 해결 이력이 있으므로 단순히 다시 켜지 않는다. 기존 오류를 재현하고 tracker/RSS/cgroup/병합 메모리를 함께 확인해 한도를 재설계한다. 쿼리 제한·서버 제한·컨테이너 제한은 서로 다른 범위다.
- background pool 4, concurrency ratio 1: 최근 active parts 7~18개로 누적 증가 증거가 없으므로 유지 권장이다. CPU만 보고 pool을 늘릴 근거가 없다.
- 원본 1개와 MV 6개의 집계는 적재 비용을 늘린다. Grafana가 30초/1분 refresh이고 이번 부하에서 parts 적체 증거가 없어 먼저 제거할 근거는 없다. 향후 CPU 병목이면 10초 집계 사용 여부부터 확인한다.
- `ORDER BY(service,event_id)`는 시간 범위 원본 조회를 직접 최적화하지는 않는다. 현재 주 조회는 시간 정렬 집계 테이블을 이용하므로 유지 가능하다. 원본 시간 검색이 주 사용처가 될 때 별도 재평가한다.

## 7. 전체 자원과 관측

- 현재 셸에 CPU 8논리 코어, 메모리 약 11GiB가 보인다. Docker daemon이 같은 예산을 쓰는지는 미확인이다.
- compose의 모든 서비스 mem_limit 합은 약 13.90GiB다. 상한 합은 즉시 예약량이 아니므로 이것만으로 OOM을 확정할 수 없다. 다만 모두 같은 WSL 메모리를 쓰고 동시에 증가하면 전체 예산을 넘길 수 있다. Docker에 실제 할당된 총 메모리 및 swap을 확인해야 한다.
- 대부분 CPU 한도가 주석이므로 Spark의 4+2코어와 ClickHouse 등의 CPU 사용은 호스트에서 경합할 수 있다. CPU 한도를 일괄 적용하기보다 실제 부하에서 CPU·GC·대기·lag를 함께 본다.
- `diag.sh`는 없는 async insert 로그 테이블 조회에서 종료되어 EPS와 지연 수집을 놓친다. 최적화 전후를 판정할 관측 경로부터 복구해야 한다.
- `scripts/validation_snapshot.sh`도 주기마다 `diag.sh all`을 호출하므로 같은 결함을 물려받는다. `diag.sh`가 `set -e` 상태에서 `diag_ch` 3번 쿼리에서 끝나면 스냅샷에 latency·EPS·parts 섹션이 기록되지 않고 lag(`kafka_spark_lag.py`)와 `docker stats`만 남는다. 이 스크립트로 검증 실행을 수집하기 전에 `diag.sh`를 먼저 고쳐야 한다.
- Grafana에는 현재 코드/DDL에 없는 DLQ 집계 테이블을 참조하는 패널이 있다. 해당 패널의 수치를 파싱 누락 검증으로 사용할 수 없다.

## 8. 실무 운영 관점의 추가 검토

현재 구성은 단일 호스트에서 5천 EPS를 검증하는 PoC로 해석하는 것이 적절하다. 앞의 “유지 가능”은 관측한 부하에서 즉시 증설할 근거가 없다는 뜻이며, 운영 전환 승인이나 장애 내구성 보장을 뜻하지 않는다. 실무에서도 비용과 허용 손실에 따라 단일 노드를 선택할 수 있으므로 모든 항목을 일괄 증설할 필요는 없다.

### 8-1. 우선 수정해야 할 운영 지표 해석

| 우선도 | 확인된 차이와 근거 | 실무에서 필요한 해석·조치 |
|---|---|---|
| 높음 | `21_materialized_views.sql`의 `created_cnt`와 `stored_cnt`는 모두 **이미 event_log에 도착한 행**에서 계산한다. | 이 비율로 생성→적재 누락을 검증할 수 없다. 도착한 이벤트의 동일 분 내 저장 비율로 명명하고, 별도 생성·전달 완료 수량과 Kafka 오프셋 대사를 사용한다. 생성 1만 건 중 5천 건만 도착해도 그 5천 건이 같은 분에 저장되면 100%가 나올 수 있다. |
| 높음 | 동일 분에 속하는지 비교하므로 분 경계 직전 생성된 이벤트는 1초 지연만으로 실패하고, 분 초에 생성된 이벤트는 수십 초 지연도 성공한다. | `stored_ratio=100%`가 지연 SLO 충족을 의미하지 않는다. 이벤트별 지연의 임계값 충족률과 p95/p99를 별도로 계산한다. |
| 높음 | 두 latency MV의 `e2e_ms`는 `kafka_received_at→clickhouse_stored_at`이다. `kafka_received_at`은 Kafka 레코드 `timestamp`를 그대로 쓴 값이고(`parse_event.py:17`), 토픽에 `message.timestamp.type` 지정이 없으므로 기본값 CreateTime, 즉 프로듀서가 `produce()`를 호출한 시각이다. 브로커 수신 시각이 아니다. | 현재 E2E를 “produce() 호출→저장”으로 명시한다. librdkafka 큐 대기·linger·전송이 포함된다. `producer_to_kafka`는 “이벤트 생성→produce() 호출”(애플리케이션 큐 대기)이며 브로커 도달 지연이 아니다. 실제 토픽의 timestamp type은 런타임에서 확인한다. 생성→저장을 원하면 `event_timestamp`부터의 차이를 이벤트별로 집계한다. 단계별 p95를 더해서 전체 p95를 만들 수 없다. Grafana 표시까지 포함하려면 조회·refresh 지연도 별도 측정한다. |
| 높음 | `logs.error` 복제도 원본과 함께 event_log 및 MV에 들어가고, 집계는 topic 구분 없이 `countState`한다. | Kafka 레코드 처리 EPS와 원본 업무 이벤트 EPS를 분리한다. 업무 오류율은 원본 토픽만 대상으로 정의한다. 실패 이벤트가 전부 한 번 더 들어가므로 집계 오류율은 실제의 약 2배가 된다(auth: 실제 0.30% → 집계 약 0.60%). `infra/monitor/watchdog/clickhouse_probe.py`의 오류율 경보도 같은 집계를 쓰므로 경보 임계값 해석도 함께 바뀐다. |
| 조건부 | 오류 집계는 `status_code >= 500`만 사용한다. 다만 `normalize_event.py:52-57`이 `status_code`가 없으면 `result='fail'`→500, `'success'`→200으로 채운다. | 현재 모드 `SIM_EVENT_MODE=domain`에서는 `status_code >= 500`이 `result='fail'`과 같아 차이가 없다. 차이는 `http`/`all` 모드에서만 생긴다(order의 실패 코드 422/409는 5xx가 아니다). 모드를 바꿀 때 업무 실패와 HTTP 5xx를 별도 지표로 정의한다. |
| 중간 | 종합 판단의 1.337초는 `writer_base.py`에서 기록한 transform/write 단계 합산이다. | 이 값은 전체 trigger 시간의 대용으로 제한적으로만 사용한다. 오프셋 확인·커밋 등까지 포함하는 StreamingQueryProgress의 `durationMs.triggerExecution`, 입력 건수, 실패·재시도와 함께 판정한다. “4초 초과 0건”도 기존 계측 범위에 한정한다. |

근거: `infra/clickhouse/sql/21_materialized_views.sql`, `infra/grafana/dashboards/ops_monitoring.json`, `spark_job/clickhouse/writer_base.py`, `log_simulator/simulator/services/{auth,order,payment}.py`.

### 8-2. 복구·가용성은 정상 부하 성능과 별도 검증이 필요하다

- **단일 장애 영역**: Kafka는 broker/controller 겸용 1노드, 토픽 RF=1이며 ClickHouse도 단일 노드다. Spark worker를 2개 띄워도 같은 호스트 장애를 견디는 구성은 아니다. 무중단·호스트 손실 내구성이 요구되면 별도 장애 영역의 복제 또는 관리형 서비스를 검토한다. Kafka의 일반적인 비교 구성은 RF=3, min ISR=2, acks=all이지만 현재 단일 브로커에 min ISR=2만 적용하면 쓰기가 실패한다. [Kafka 3.6 공식 설정](https://kafka.apache.org/36/configuration/broker-configs/).
- **볼륨 보존과 백업은 다르다**: Kafka 로그·메타데이터, Spark 체크포인트, ClickHouse가 `/data/log-etlm` 아래 호스트 경로에 있다. 컨테이너 재생성에는 유용하지만 호스트·디스크 손실의 복구 근거는 아니다. 저장소 밖 백업 존재 여부는 미확인이므로 RPO/RTO를 정하고 실제 복원 시험으로 확인한다.
- **retention은 중단 시간만으로 정하지 않는다**: 장애 탐지·대응·복구 후 backlog 회수까지 여유가 필요하다. 앞의 30만 건/618초는 “지속 유입 중에도 매 4초마다 22,000건을 완료”하는 가정이다. 운영 상한은 목표 복구 시간에 맞춰 회수 능력을 실측하고, 디스크도 실제 메시지 크기·압축률·복제 수·최대 유입률로 산정한다.
- **체크포인트 보존만으로 배포 호환성이 보장되지 않는다**: 소스·쿼리 변경이 기존 체크포인트와 호환되는지 검증하고, 필요하면 새 체크포인트와 명시적 시작 오프셋을 사용한다. 재처리 범위, 중복 처리, 중단·롤백 절차까지 정해야 한다.
- **원본 중복 제거와 집계 정합성은 별개다**: 나중에 원본 테이블만 중복 제거해도 이미 MV에 누적된 count/quantile이 자동 교정되는 것으로 가정하면 안 된다. 재처리 전에 집계로 전달될 중복을 차단하거나 영향 구간의 집계 재구축을 설계한다. ClickHouse incremental MV는 INSERT 블록을 처리한다. [공식 MV 설명](https://clickhouse.com/docs/concepts/features/materialized-views/incremental-materialized-view).

### 8-3. 배포와 접근 제어에서 PoC 가정을 분리해야 한다

- **DDL 재실행에 데이터 삭제가 포함된다**: `infra/clickhouse/sql/10_fact.sql:2`는 `DROP TABLE IF EXISTS analytics.event_log`로 시작한다. compose에서 초기화 스크립트로 마운트한다. 기존 볼륨에서 매 재시작마다 실행된다고 단정할 수는 없으나, 이 파일을 운영 마이그레이션으로 수동 재실행하면 원본 데이터가 삭제된다. 원본만 삭제하고 집계가 남으면 지표도 불일치한다. 운영 변경은 비파괴 ALTER·별도 버전 테이블·검증된 이관 및 롤백으로 분리한다.
- **인증과 네트워크 경계**: compose의 Kafka 외부 리스너는 PLAINTEXT이고 `29092`, ClickHouse HTTP는 `8123`으로 호스트 주소 제한 없이 게시되어 있다. `log_user`는 XML에서 전체 IP 허용과 `access_management=1`을 사용한다. 방화벽·실효 권한은 미확인이므로 “외부 누구나 접근 가능”으로 단정하지 않되, 배포 전 사설망/바인딩, 필요한 인증·암호화, ingest 계정의 최소 권한과 관리자 계정 분리를 확인한다.
- **재현성과 변경 관리**: Kafka UI와 CH UI는 `latest` 이미지다. 실제 사용하는 도구라면 버전/다이제스트를 고정하고 변경 전후 호환성을 검증한다. 주요 서비스의 restart 정책도 주석 처리된 항목이 있어 Spark driver 하나의 자동 재시작만으로 전체 자동 복구를 주장할 수 없다.

### 8-4. 실무 검증에서 추가할 합격 조건

1. 원본 이벤트와 오류 복제를 구분한 입력·적재 대사, 파싱 실패 레코드 수량(집계 방식은 5절의 실측 결과로 결정), 생성→저장 지연 SLO, 허용 중복·손실과 RPO/RTO를 먼저 정의한다. 수치는 업무 요구로 결정한다.
2. 정상 부하 외에 짧은 burst, Kafka 중단, sink 지연·중단, Spark 강제 종료 후 복구, 재처리와 배포를 시험한다. 대기열을 줄인 뒤에는 생성량 감소와 실제 유실을 구분한다. 시뮬레이터가 스스로 부하를 낮춘 결과를 5천 EPS 처리 성공으로 판정하지 않는다.
3. 중복·누락 검증은 최대 오프셋만 비교하지 않는다. 파티션별 검증 구간에서 실제 전달 대상 레코드를 대사해 내부 누락과 중복을 확인한다. 오프셋 범위 길이가 항상 데이터 레코드 수와 같다고 가정하지 않는다.
4. 장시간 시험은 peak·보관 주기·병합/TTL·백업·조회 동시 부하를 포함하도록 정한다. 현재 약 2시간 관측만으로 원본 3일 및 집계 7일 TTL에 따른 디스크 정상 상태를 검증한 것은 아니다. 24시간이라는 고정 숫자만 채우는 것도 충분조건이 아니다.
5. Watchdog은 컨테이너 감시 외에 `clickhouse_probe.py`로 EPS 하한, freshness, 단계별·E2E p95, 오류율 경보를 이미 갖고 있다. 추가할 것은 Kafka lag 증가, 디스크 여유, async 실패(실효일 때)다. compose에 monitor 서비스가 없으므로 실제 가동 여부를 확인하고 경보 전달 및 대응 절차를 시험한다. 오류율 경보는 복제를 포함한 집계를 쓰므로 8-1의 정정과 함께 임계값을 다시 정한다. 저장 지표는 성공한 이벤트만 관측하므로 “데이터가 아예 오지 않음”이 freshness·EPS 하한 경보로 실제 탐지되는지도 시험한다.

**추가 검토 결론**: 앞의 튜닝 후보는 PoC 기준으로 대체로 타당하지만, 실무 판단에서는 지표 정의와 데이터 보존·복구 절차가 먼저다. 특히 적재 비율, E2E 명칭, 복제로 부풀려진 업무 지표, 파괴적 DDL 재실행은 현재 문서를 그대로 운영 기준으로 사용하기 전에 바로잡아야 한다. 이 절은 검토 결과이며 운영 구성 변경이나 장애 시험을 수행했다는 의미는 아니다.

## 권장 순서

4초/22,000/파티션 2/JDBC 15,000/background pool 4는 아래 전 단계에서 유지한다. 각 단계는 한 번에 하나씩 적용하고, 전달 의미가 달라지는 변경 뒤에는 성능 결과를 다시 측정한다. 설정만 바꾸는 단계와 코드 변경이 필요한 단계를 구분해 표시했다.

1. **관측 복구(코드)**: `diag.sh`가 없는 테이블 조회에서 중단하지 않게 한다. `validation_snapshot.sh`도 이 스크립트에 의존하므로, 이것이 없으면 이후 모든 전후 비교가 불가능하다.
2. **실효 설정 수집(변경 없음)**: 현재 값을 기준선으로 보존하고 실행 시 유효 설정·버전·DDL을 수집한다. 특히 async insert 실효 여부(6절), 토픽 파티션·retention·timestamp type, log_user의 실효 프로파일·권한, 깨진 JSON의 처리 동작(5절)을 확인한다.
3. **지표 정의 정정(DDL·대시보드)**: 오류 복제를 업무 지표에서 분리하고, `created/stored` 비율과 E2E 구간의 명칭을 실제 의미에 맞춘다(8-1). 이후 측정값의 의미가 여기서 정해진다.
4. **체크포인트 보존(설정)**: `SPARK_RESET_CHECKPOINT_ON_START=false`만 단독으로 적용하고 재시작 복구를 확인한다. 처리량 영향이 없는 변경이다.
5. **sink 완료 응답(설정)**: 2번에서 async가 실효로 확인된 경우에만 동기 INSERT와 async+wait를 하나씩 비교한다. 실효가 아니면 건너뛴다.
6. **producer 수신 확인**: `acks=all`과 멱등성 활성화(설정)를 먼저 적용하고, 그 뒤 delivery callback 결과를 성공 통계에 반영한다(코드). 파싱 실패 레코드 집계(코드)는 2번의 실측 결과에 맞춰 설계한다.
7. **애플리케이션 큐 축소(설정)**: 16~32개 목록 후보로 줄여 backpressure 이전 메모리 한계와 실제 생성량을 확인한다. Kafka 중단·Spark 강제 종료 등 8-4의 장애 시험은 이 단계 뒤에 수행한다.
8. **자원 튜닝**: driver 메모리 여유, 불필요한 GC 고정값 순서로 한 조건씩 실험한다.

최적값 판단은 전체 trigger p95/p99, 실제 입력·적재 EPS, Kafka 누적 lag, E2E p95/p99, 누락·중복, 자원 사용량으로 한다. 파괴적 DDL 재실행 방지와 접근 제어(8-3)는 실험 순서와 별개로 운영 전환 전에 해결할 조건이다.

## 공식 참고 자료

- [librdkafka 설정](https://docs.confluent.io/platform/current/clients/librdkafka/html/md_CONFIGURATION.html): acks, idempotence, linger, producer batch 상한.
- [Kafka 7.6 broker 설정](https://docs.confluent.io/platform/7.6/installation/configuration/broker-configs.html): retention, 세그먼트, 파티션별 저장 한도.
- [Spark 4.0.1 Kafka 연동](https://spark.apache.org/docs/4.0.1/streaming/structured-streaming-kafka-integration.html): 트리거 전체 입력 상한과 오프셋 복구 의미.
- [Spark 4.0.1 설정](https://spark.apache.org/docs/4.0.1/configuration.html), [Standalone](https://spark.apache.org/docs/4.0.1/spark-standalone.html): 메모리 범위, overhead 적용 대상, executor 코어 할당.
- [PySpark 4.0.1 gateway 소스](https://github.com/apache/spark/blob/v4.0.1/python/pyspark/java_gateway.py): 최초 JVM 실행 시 conf 전달.
- [ClickHouse async insert](https://clickhouse.com/docs/concepts/features/operations/insert/asyncinserts): flush 완료 응답과 실패 전달, adaptive timeout.
- [ClickHouse insert 전략](https://clickhouse.com/docs/concepts/best-practices/selecting-an-insert-strategy): 클라이언트 배치와 INSERT 크기.

공식 문서의 최신 ClickHouse/librdkafka 기본값은 실행 이미지의 실제 값으로 간주하지 않았다. 적용 중인 값은 런타임에서 확인해야 한다.
