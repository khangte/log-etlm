# -----------------------------------------------------------------------------
# 파일명 : tests/simulator/test_event_id.py
# 목적   : event_id 생성 규칙 회귀 가드 (seed 포맷이 실수로 바뀌면 즉시 실패)
# 실행   : uv run pytest tests/simulator/test_event_id.py -v
# -----------------------------------------------------------------------------

from __future__ import annotations

from log_simulator.simulator.base import BaseServiceSimulator


def test_generate_stable_event_id_고정_규칙():
    sim = BaseServiceSimulator(routes=[{"weight": 1}], profile={})
    event = {
        "service": "auth",
        "event_name": "auth.login_completed",
        "request_id": "req_abc123",
        "ts_ms": 1700000000000,
        "route_template": "/auth/login",
        "method": "POST",
        "result": "success",
        "status_code": 200,
        "duration_ms": 42,
        "user_id": 7,
    }
    assert sim.generate_stable_event_id(event) == "evt_693c26a0eef96e3f28631d18f0d5571b"
