from __future__ import annotations

import time


def now_ms() -> int:
    return int(time.time() * 1000)


def floor_minute_ms(ts_ms: int) -> int:
    return ts_ms - (ts_ms % 60_000)

