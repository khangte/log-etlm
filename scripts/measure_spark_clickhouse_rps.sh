#!/usr/bin/env bash
set -euo pipefail

SPARK_CONTAINER="${SPARK_CONTAINER:-spark}"
N="${1:-20}"

# Uses docker log timestamps to estimate throughput between successful ClickHouse batches.
# Output columns:
#   end_time | rows | dt_sec | rps

docker logs --timestamps --tail 5000 "${SPARK_CONTAINER}" 2>/dev/null \
  | grep -a -F "Batch 성공" \
  | tail -n "$((N + 1))" \
  | python3 -c '
import re
import sys
from datetime import datetime, timezone

row_re = re.compile(r"rows=(\d+)")

def parse_ts(raw: str) -> datetime:
    # docker logs --timestamps: RFC3339Nano like 2025-12-16T06:14:59.123456789Z
    raw = raw.strip()
    if raw.endswith("Z"):
        raw = raw[:-1] + "+00:00"
    if "." in raw:
        base, frac = raw.split(".", 1)
        frac, tz = frac.split("+", 1)
        frac = (frac + "000000")[:6]  # nanoseconds -> microseconds
        raw = f"{base}.{frac}+{tz}"
    return datetime.fromisoformat(raw)

lines = [ln.rstrip("\n") for ln in sys.stdin if ln.strip()]
events = []
for ln in lines:
    ts = ln.split(" ", 1)[0]
    m = row_re.search(ln)
    if not m:
        continue
    events.append((parse_ts(ts), int(m.group(1))))

if len(events) < 2:
    print("not enough batch logs (need >=2).")
    sys.exit(0)

total_rows = 0
total_dt = 0.0
for (t0, _rows0), (t1, rows1) in zip(events, events[1:]):
    dt = (t1 - t0).total_seconds()
    if dt <= 0:
        continue
    rps = rows1 / dt
    total_rows += rows1
    total_dt += dt
    end = t1.astimezone(timezone.utc).isoformat()
    print(f"{end}\trows={rows1}\tdt={dt:.2f}s\trps={rps:.1f}")

if total_dt > 0:
    print(f"avg\trows={total_rows}\tdt={total_dt:.2f}s\trps={total_rows/total_dt:.1f}")
'
