# -----------------------------------------------------------------------------
# 파일명 : tests/simulator/test_api_simulator.py
# 목적   : simulator API 엔드포인트 기능 테스트
# 실행   : uv run pytest tests/simulator/test_api_simulator.py -v
# -----------------------------------------------------------------------------

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi.testclient import TestClient
from fastapi import FastAPI

from log_simulator.api.simulator import router


@pytest.fixture()
def app():
    """라우터만 등록한 테스트용 FastAPI 앱."""
    _app = FastAPI()
    _app.include_router(router)
    return _app


@pytest.fixture()
def client(app):
    return TestClient(app)


# ---------------------------------------------------------------------------
# GET /simulator/ping
# ---------------------------------------------------------------------------

class TestPing:
    def test_정상응답(self, client):
        # Act
        res = client.get("/simulator/ping")

        # Assert
        assert res.status_code == 200
        assert res.json() == {"status": "ok"}


# ---------------------------------------------------------------------------
# PATCH /simulator/eps
# ---------------------------------------------------------------------------

class TestUpdateEps:
    def test_정상_eps_변경(self, client):
        # Arrange
        with patch("log_simulator.api.simulator.engine") as mock_engine:
            mock_engine.set_eps = AsyncMock()

            # Act
            res = client.patch("/simulator/eps", params={"value": 10000})

        # Assert
        assert res.status_code == 200
        assert res.json() == {"status": "ok", "eps": 10000.0}
        mock_engine.set_eps.assert_awaited_once_with(10000.0)

    def test_eps_0_이하_422(self, client):
        # Act
        res = client.patch("/simulator/eps", params={"value": 0})

        # Assert
        assert res.status_code == 422

    def test_eps_음수_422(self, client):
        # Act
        res = client.patch("/simulator/eps", params={"value": -1})

        # Assert
        assert res.status_code == 422

    def test_엔진_미시작_503(self, client):
        # Arrange
        with patch("log_simulator.api.simulator.engine") as mock_engine:
            mock_engine.set_eps = AsyncMock(side_effect=RuntimeError("엔진이 실행 중이 아닙니다."))

            # Act
            res = client.patch("/simulator/eps", params={"value": 5000})

        # Assert
        assert res.status_code == 503
        assert "엔진이 실행 중이 아닙니다" in res.json()["detail"]

    def test_소수점_eps_허용(self, client):
        # Arrange
        with patch("log_simulator.api.simulator.engine") as mock_engine:
            mock_engine.set_eps = AsyncMock()

            # Act
            res = client.patch("/simulator/eps", params={"value": 3500.5})

        # Assert
        assert res.status_code == 200
        assert res.json()["eps"] == 3500.5

    def test_value_파라미터_누락_422(self, client):
        # Act
        res = client.patch("/simulator/eps")

        # Assert
        assert res.status_code == 422
