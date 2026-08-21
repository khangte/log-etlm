# 우선순위 8번 설계 검토 결과 — task_builder 중복 병합

- 요청: lead
- 검토일: 2026-08-21
- 대상: docs/overengineering_audit.md line 23 / log_simulator/simulator/task_builder.py

## 현재 코드 재확인

6·7번 라운드의 파라미터 제거와 `lru_cache` 전환(커밋 `64d259a` 등)이 반영된
현재 상태에서 두 함수를 다시 비교했다. 결과적으로 **감사 시점보다 병합이 더 쉬워졌다.**

주입 파라미터가 사라지면서 `resolved_*` 중간 변수가 없어졌고, 두 함수 모두
본문 첫 두 줄이 `get_simulator_settings()` / `get_queue_config()` 무인자 호출로
같아졌다. 지금 두 함수의 차이는 다음 세 가지뿐이다.

| | create_simulator_tasks | create_service_tasks_only |
| --- | --- | --- |
| `publish_queue` | 직접 생성 (`maxsize=settings.queue_size`) | 인자로 받음 |
| 반환 | `(publish_queue, service_tasks)` | `service_tasks` |
| 루프 본문 | — | **완전히 동일** |

`available_services` 계산부터 `service_tasks.append(task)`까지 약 20줄이
공백·순서·태스크 이름(`service-loop-{service}-{idx}`)까지 글자 단위로 같다.
병합에 걸림돌이 되는 차이는 없다.

## 결론

감사 제안대로 병합한다. `publish_queue`를 인자로 받는 함수 하나만 남기고
큐 생성은 호출부로 올린다. 88줄 → 약 50줄.

## 설계

**log_simulator/simulator/task_builder.py**

```python
def create_service_tasks(
    simulators: Dict[str, Any],
    base_eps: float,
    service_eps: Dict[str, float],
    publish_queue: "asyncio.Queue[list[BatchMessage]]",
) -> List[asyncio.Task]:
    """주어진 publish_queue에 붙는 서비스 루프 태스크를 생성한다."""
```

- 남기는 본문은 현재 `create_service_tasks_only`의 것과 동일하다.
  새로 짜지 말고 그대로 둔 뒤 `create_simulator_tasks`를 지우는 방향이 안전하다.
- 이름은 `create_service_tasks`로 바꾼다. 함수가 하나만 남으면
  `_only` 접미사가 대비할 상대를 잃는다.
- `Tuple` 반환이 사라지므로 `typing` import에서 `Tuple`을 뺀다.

**log_simulator/pipeline_builder.py**

```python
def assemble_pipeline(...) -> Pipeline:
    publish_queue: asyncio.Queue[list[BatchMessage]] = asyncio.Queue(
        maxsize=get_simulator_settings().queue_size
    )
    service_tasks = create_service_tasks(
        simulators=simulators,
        base_eps=base_eps,
        service_eps=service_eps,
        publish_queue=publish_queue,
    )
    stats_queue: asyncio.Queue[tuple[str, int]] = asyncio.Queue()
    ...
```

`from .simulator.settings import get_simulator_settings`를 추가한다.

큐 생성을 이 파일로 올리는 것이 오히려 자연스럽다. `assemble_pipeline`은
이미 `stats_queue`를 여기서 만들고 있어서, 두 큐가 같은 자리에서 생기게 된다.
지금은 큐 하나는 하위 함수 안에서, 다른 하나는 조립 함수에서 만들어져 비대칭이다.

`create_publish_queue()` 같은 2줄짜리 헬퍼를 task_builder에 두는 안도 검토했으나
채택하지 않는다. 호출자가 하나뿐인 래퍼로, 이번 감사에서 계속 걷어내고 있는
바로 그 형태다.

## 호출부 영향

두 곳뿐이고 둘 다 기계적이다.

- `log_simulator/pipeline_builder.py:31-35` — 위 설계대로 수정.
  `create_simulator_tasks` import를 `create_service_tasks`로 교체.
- `log_simulator/engine.py:94-100` — `set_eps()`가 이미
  `publish_queue=self._pipe.publish_queue`를 넘기고 있으므로 **이름만 바뀐다.**
  인자 구성은 그대로다.

`engine.py:94`의 함수 지역 import는 모듈 상단으로 올린다. 순환 참조 회피용으로
보이지만 실제 순환은 없다 — `task_builder`는 `settings`, `stream_pipeline`,
`models.messages`만 import하고 `engine`을 참조하지 않는다.
같은 커밋에서 함께 정리한다.

## 동작 차이

없다. 태스크 생성 순서, 개수, 이름(`service-loop-{service}-{idx}`),
`per_loop_eps` 계산, 큐 `maxsize` 모두 동일하다.
`maxsize`에 쓰는 `get_simulator_settings()`는 `lru_cache`로 같은 객체를 돌려주므로
읽는 위치가 바뀌어도 값이 달라지지 않는다.

`set_eps()`가 기존 큐를 재사용하는 성질도 그대로다. 오히려 큐를 만드는 함수와
재사용하는 함수가 분리되면서, "EPS 변경 시 큐를 새로 만들면 안 된다"는 제약이
시그니처에 드러난다.

## 테스트 영향

없다. `tests/simulator/test_api_simulator.py`는 `log_simulator.api.simulator.engine`
객체를 통째로 `patch`하므로 이 함수들에 닿지 않는다. 다른 테스트도 마찬가지다.

## 커밋 분리

`refactor(simulator)` 한 건. task_builder, pipeline_builder, engine 세 파일이
하나의 시그니처 변경으로 묶여 있어 쪼개면 중간 커밋이 import 에러로 깨진다.
