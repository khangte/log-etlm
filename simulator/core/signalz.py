# -----------------------------------------------------------------------------
# 파일명 : simulator/core/signalz.py
# 목적   : 종료 시그널(SIGINT/SIGTERM) 처리 도우미
# 사용   : runner에서 with-style 또는 폴링으로 should_stop() 확인
# -----------------------------------------------------------------------------

from __future__ import annotations
import signal
from threading import Event


class GracefulKiller:
    """
    SIGINT/SIGTERM을 받아 종료 이벤트를 세트하는 헬퍼.

    사용:
        killer = GracefulKiller()
        while not killer.should_stop():
            ...
    """
    def __init__(self) -> None:
        self._stop = Event()
        signal.signal(signal.SIGINT, self._handle)
        signal.signal(signal.SIGTERM, self._handle)

    def _handle(self, signum, frame) -> None:
        self._stop.set()

    def should_stop(self) -> bool:
        """종료 요청이 들어왔는지 여부."""
        return self._stop.is_set()
