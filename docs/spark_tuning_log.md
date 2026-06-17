# Spark 스트리밍 튜닝 실험 로그

EPS 5000 고정 환경에서 4초 트리거 기준 배치 처리 시간을 측정한 결과를 기록한다.
각 실험은 `batch_timing.log`의 `transform` + `write` 단계 시간을 기준으로 평가한다.

> **측정 제외 항목**: Kafka read 시간은 foreachBatch 호출 전에 발생하므로 측정값에 포함되지 않는다.

---

## 환경

- EPS: 5,000 (고정)
- Kafka 파티션: 7개 (auth:3, order:2, payment:1, error:1)
- 트리거 인터벌: 4초
- 이벤트 평균 크기: ~310 bytes
- 배치당 이벤트: ~20,000개

---

## 실험 기록

### 실험 1 — 베이스라인 (3코어 + write=3)

| 항목 | 값 |
|---|---|
| 날짜 | 2026-06-17 |
| spark-worker-1 코어 | 3 |
| WRITE_PARTITIONS | 3 |
| PRE_COALESCE_PARTITIONS | 3 |
| JDBC compress | 0 (비압축) |

**결과** (7,233배치 전체 기준)

| 지표 | 값 |
|---|---|
| 평균 transform | 1.52s |
| 평균 write | 2.02s |
| 평균 총시간 | 3.54s |
| 4s 초과 비율 | 22.9% |
| 최대 총시간 | 277s (캐스케이딩) |

**관찰**
- write가 평균 2.02s로 주 병목
- 4s 초과 시 캐스케이딩 발생 → 누적 지연

---

### 실험 2 — 4코어 + write=2 + compress=1

| 항목 | 값 |
|---|---|
| 날짜 | 2026-06-17 |
| spark-worker-1 코어 | 4 |
| WRITE_PARTITIONS | 2 |
| PRE_COALESCE_PARTITIONS | 2 |
| JDBC compress | 1 (LZ4) |

**결과** (55배치, 12:29~12:33 세션 기준)

| 지표 | 값 |
|---|---|
| 평균 transform | 1.63s |
| 평균 write | 1.28s |
| 평균 총시간 | 2.92s |
| 4s 초과 비율 | 20.0% |
| 최대 총시간 | 6.15s |

**관찰**
- write 시간 2.02s → 1.28s로 감소 ✅
- transform 스파이크(3~4s)가 여전히 발생 → 캐스케이딩 재발
- 병목이 write → transform으로 이동
- transform 스파이크 원인 추정: executor JVM GC 또는 CPU 경합

---

### 실험 3 — 트리거 6s + write=2 + compress=1

| 항목 | 값 |
|---|---|
| 날짜 | 2026-06-17 |
| spark-worker-1 코어 | 4 |
| WRITE_PARTITIONS | 2 |
| PRE_COALESCE_PARTITIONS | 2 |
| 트리거 인터벌 | 6초 |
| SPARK_MAX_OFFSETS_CAP | 40,000 |
| SPARK_CLICKHOUSE_JDBC_BATCHSIZE | 15,000 |
| JDBC compress | 1 (LZ4) |

**결과** (54배치, 12:52~ 세션 기준)

| 지표 | 값 |
|---|---|
| 평균 transform | 1.19s |
| 평균 write | 0.94s |
| 평균 총시간 | 2.13s |
| 6s 초과 비율 | 1.9% (1개) |
| 최대 총시간 | 7.36s |

**관찰**
- 평균 총시간 2.92s → 2.13s로 단축 ✅
- 6s 초과 비율 1.9% → 사실상 캐스케이딩 없음 ✅
- write 0.94s로 안정 (batchsize 15k → 파티션당 1회 flush)
- transform 스파이크는 산발적으로 존재하나 6s 여유 내에서 흡수됨

---

### 실험 4 — G1GC 적용

| 항목 | 값 |
|---|---|
| 날짜 | 2026-06-17 |
| spark-worker-1 코어 | 4 |
| WRITE_PARTITIONS | 2 |
| PRE_COALESCE_PARTITIONS | 2 |
| 트리거 인터벌 | 6초 |
| 기타 변경 | `infra/spark/spark-defaults.conf` 마운트, executor G1GC 적용 |

**결과** (69배치, 13:12~ 세션 기준)

| 지표 | 값 |
|---|---|
| 평균 transform | 1.04s |
| 평균 write | 0.88s |
| 평균 총시간 | 1.92s |
| 6s 초과 비율 | 0.0% ✅ |
| 최대 총시간 | 4.62s |

**관찰**
- transform 평균 1.19s → 1.04s로 단축, 스파이크 소멸 ✅
- 6s 초과 배치 0건 (실험 3: 1.9%) ✅
- 최대 총시간 7.36s → 4.62s로 감소 (GC pause 제거 효과)
- G1GC의 예측 가능한 pause 덕분에 transform 분산이 균일해짐

---

### 실험 5 — 트리거 5s (G1GC 유지)

| 항목 | 값 |
|---|---|
| 날짜 | 2026-06-17 |
| spark-worker-1 코어 | 4 |
| WRITE_PARTITIONS | 2 |
| PRE_COALESCE_PARTITIONS | 2 |
| 트리거 인터벌 | 5초 |
| 기타 변경 | G1GC 유지 |

**결과** (100배치, 13:30~ 세션 기준)

| 지표 | 값 |
|---|---|
| 평균 transform | 1.29s |
| 평균 write | 0.89s |
| 평균 총시간 | 2.18s |
| 5s 초과 비율 | 5.0% (5개) ❌ |
| 최대 총시간 | 9.90s ❌ |

**관찰**
- batch 72~75 구간에서 transform 스파이크 → 연속 캐스케이딩 (30초간 누적 지연)
- batch 86에서도 단발 초과 (5.18s)
- 평균은 2.18s로 여유 있어 보이나, GC 스파이크 발생 시 5s 여유가 흡수하지 못함
- 5s 트리거는 실험 4(6s)의 최대값(4.62s)과 겹쳐 캐스케이딩 구조적으로 발생
- **결론: 6s 트리거로 복구** — 1s 레이턴시 이득보다 캐스케이딩 리스크가 큼 → 6s로 복구 완료

---

### 실험 6 — ClickHouse OOM 수정 + 6s 트리거 복구

| 항목 | 값 |
|---|---|
| 날짜 | 2026-06-18 |
| spark-worker-1 코어 | 4 |
| WRITE_PARTITIONS | 2 |
| PRE_COALESCE_PARTITIONS | 2 |
| 트리거 인터벌 | 6초 |
| ClickHouse 변경 | `background_pool_size` 8→4, `max_server_memory_usage=0` (자체 한도 비활성화) |

**결과** (100배치, 15:05~15:15 세션 기준)

| 지표 | 값 |
|---|---|
| 평균 transform | 1.15s |
| 평균 write | 0.94s |
| 평균 총시간 | 2.09s |
| 6s 초과 비율 | 0.0% ✅ |
| 최대 총시간 | 4.84s |

**관찰**
- 실험 4(최적 기준) 대비 소폭 상승 (JVM 워밍업 영향)하나 기준 내 안정 ✅
- ClickHouse MEMORY_LIMIT_EXCEEDED 재발 없음 ✅
- OOM 근본 원인: mmap 기반 background merge로 인한 트래커-RSS 괴리 (트래커 ~4.5 GiB vs RSS ~1 GiB)
- `max_server_memory_usage=0` + `max_server_memory_usage_to_ram_ratio=0` 으로 ClickHouse 자체 한도 비활성화, Docker `mem_limit: 5g`가 실제 보호 역할

---

## 다음 튜닝 후보

| 항목 | 내용 | 기대 효과 |
|---|---|---|
| executor 메모리 증가 | 1g → 1.2g, worker --memory 1700m | GC 빈도 감소 |
| ClickHouse native connector | JDBC → HTTP bulk insert | write 추가 단축 (현재 우선순위 낮음) |
