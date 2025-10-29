# generator.py
import asyncio
import json
from datetime import datetime, timezone
from simulator import get_all_simulators
from .schedular import SEOUL, interval_for_now, batch_for_service

async def process_service(service, simulator, output_plugin, seoul_now):
    """
    한 서비스에 대해, 현재 시간대 기준 배치 개수만큼 로그 생성 후 전송.
    simulator.generate_logs(now, count) / simulator.render(log) 시그니처 가정.
    """
    # 시간대/서비스별 생성 개수
    count = batch_for_service(seoul_now, service)

    # ClickHouse/Spark 등은 UTC 타임스탬프가 편하므로 UTC 기준 now도 넘겨줌
    utc_now = seoul_now.astimezone(timezone.utc).replace(tzinfo=timezone.utc)

    events = [simulator.generate(utc_now) for _ in range(count)]
    for ev in events:
        rendered = json.dumps(ev, ensure_ascii=False)
        # topic 예: logs.order / logs.payment ...
        await output_plugin.write(rendered, topic=f"logs.{service}")

    # 과도한 busy-loop 방지(짧은 양념 슬립)
    await asyncio.sleep(0)

async def start_log_generator(output_plugin):
    """
    모든 서비스 시뮬레이터를 병렬로 돌리면서,
    루프 간격은 시간대별로 interval_for_now() 값을 적용.
    """
    simulators = get_all_simulators()
    services = list(simulators.keys())

    while True:
        seoul_now = datetime.now(SEOUL)

        # 서비스별 배치를 병렬 실행
        await asyncio.gather(*[
            process_service(svc, simulators[svc], output_plugin, seoul_now)
            for svc in services
        ])

        # 시간대별 루프 간격 적용
        await asyncio.sleep(interval_for_now(seoul_now))
