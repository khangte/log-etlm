# 우선순위 3번 설계 검토 결과 — JsonProgressListener 삭제

- 요청: lead
- 검토일: 2026-08-21
- 대상: docs/overengineering_audit.md line 19 / spark_job/main.py:19-81

## 결론

삭제한다. 정보 손실 없음.

## 감사 문서 정정

"3중 기록"은 부정확하다. 실제로는 **2중**이다.

- `batch_timing.log`는 `foreachBatch` 안에서 측정한 배치별 transform/write 단계
  소요 시간을 기록한다(`spark_job/clickhouse/writer_base.py:68,74,95,114`).
  `StreamingQueryProgress`와 측정 지점도 내용도 다르고,
  `docs/spark_tuning_log.md`의 실험 평가 기준이 바로 이 파일이다.
  중복이 아니라 보완 관계이므로 **유지한다.**
- 실제 중복은 `JsonProgressListener` ↔ Spark eventLog 두 경로다.

## 정보 손실 검토

`spark_job/spark.py:136-137`에서 `spark.eventLog.enabled=true`,
`spark.eventLog.dir=/data/log-etlm/spark-events`가 하드코딩되어 항상 켜져 있다.
`StreamingQueryListener`의 세 이벤트는 모두 `SparkListenerEvent`이므로
eventLog에 그대로 직렬화된다(Spark 3.0의 History Server Structured Streaming 탭이
이 이벤트를 근거로 동작한다).

| 리스너가 기록하는 것 | eventLog 대체 |
| --- | --- |
| `onQueryStarted` (id, runId, name) | `QueryStartedEvent` — 동일 필드 |
| `onQueryProgress` (`event.progress.json` 전문) | `QueryProgressEvent`가 같은 `StreamingQueryProgress`를 보유 |
| `onQueryTerminated` (id, runId, exception) | `QueryTerminatedEvent` — exception 포함 |

리스너만 갖는 것은 기록 시각 `ts` 하나인데, progress 페이로드 자체에 `timestamp`가
들어 있으므로 이 역시 손실이 아니다. 두 경로 모두 같은 디렉터리
`/data/log-etlm/spark-events`에 쓰므로 장애 격리 이점도 없다.

여기에 더해 `spark.sql.streaming.ui.enabled=true`(`spark_job/spark.py:61`)로
Spark UI의 Structured Streaming 탭에서 최근 progress를 실시간 조회할 수 있다.

실제로 잃는 것은 정보가 아니라 조회 편의뿐이다. 전용 JSON lines 파일은
바로 `jq`에 넣을 수 있지만, eventLog는 다른 Spark 이벤트와 섞여 있어 한 단계
필터가 더 필요하다. 대체 명령은 아래와 같고, README에 남겨 조회 수단이
조용히 사라지지 않게 한다.

```bash
grep QueryProgressEvent /data/log-etlm/spark-events/<app-id> | jq .progress
```

## 삭제하는 편이 나은 추가 이유

`onQueryProgress`는 트리거마다(현재 4초) 호출되어 분당 15회, 매번 파일을
열고 닫으며 수 KB의 JSON을 드라이버에서 동기로 쓴다. 리스너 버스 스레드에서
일어나는 일이고, 드라이버는 `mem_limit 1500m`에 과거 OOM 튜닝 이력이 있다.
또한 이 파일에는 회전 정책이 없어 무한히 커진다(컨테이너 로그는 json-file
`max-size 10m`로 제한되지만 이 경로는 호스트 볼륨이다).

## 삭제 범위

**spark_job/main.py**

1. `_progress_log_path()` (19-21행)
2. `JsonProgressListener` 클래스 (24-81행)
3. `spark.streams.addListener(JsonProgressListener(_progress_log_path()))` (92행)
4. import 정리
   - `from pyspark.sql.streaming import StreamingQueryException, StreamingQueryListener`
     → `StreamingQueryException`만 남긴다
   - `import json`, `from datetime import datetime, timezone` 삭제 (리스너 전용)
   - `import os`는 **유지** — `get_env_str(os.environ, "SPARK_MASTER_URL")`에서 계속 쓴다
   - `import logging`도 유지

약 67줄 삭제, `main.py`는 약 45줄이 된다.

**README.md**

- 169행 `SPARK_BATCH_TIMING_LOG_PATH`, `SPARK_PROGRESS_LOG_PATH` 중
  `SPARK_PROGRESS_LOG_PATH` 제거
- 위의 eventLog 조회 명령을 운용 팁에 추가

**설정 변경 없음**

`SPARK_PROGRESS_LOG_PATH`는 `docker-compose.yml`에 정의된 적이 없다.
`main.py`의 `os.getenv` 기본값으로만 존재했으므로 compose 수정은 불필요하다.

## 착수 전 확인

저장소 안에서 `spark_progress.log`를 읽는 코드·스크립트·대시보드는 없다
(grep 결과 참조부는 `main.py`와 README뿐, `crontab -l`도 비어 있음).
다만 이 검토는 WSL에서 수행해 `/data/log-etlm/spark-events`를 직접 볼 수 없었다.
developer는 VM에서 이 파일을 소비하는 임시 도구가 없는지 한 번 확인한 뒤 삭제한다.

## 커밋 분리

- `refactor(spark)`: `spark_job/main.py`
- `docs`: README.md
