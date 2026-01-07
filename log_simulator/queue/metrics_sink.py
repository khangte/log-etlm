# -----------------------------------------------------------------------------
# file: log_simulator/queue/metrics_sink.py
# purpose: lightweight metrics emitter for queue-based metrics
# -----------------------------------------------------------------------------

from __future__ import annotations

import queue as std_queue
from typing import Any


def emit_metric(metrics_queue: Any | None, name: str, value: float, service: str | None) -> None:
    if metrics_queue is None:
        return
    try:
        metrics_queue.put_nowait((name, value, service))
    except std_queue.Full:
        return
    except AttributeError:
        try:
            metrics_queue.put((name, value, service), block=False)
            return
        except TypeError:
            try:
                metrics_queue.put((name, value, service))
                return
            except Exception:
                return
        except Exception:
            return
    except Exception:
        return
