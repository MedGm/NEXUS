import time
import random
from datetime import datetime

_burst_active: bool = False
_burst_end: float = 0.0


def rate_multiplier(dt: datetime | None = None) -> float:
    global _burst_active, _burst_end

    now = time.monotonic()
    if dt is None:
        dt = datetime.now()

    multiplier = 1.0

    if dt.weekday() >= 5:          # Saturday=5, Sunday=6
        multiplier *= 0.3
    elif 9 <= dt.hour < 18:        # business hours
        multiplier *= 3.0

    # Micro-burst: Poisson arrival λ=0.05/s, duration 5–30s
    if not _burst_active and random.random() < 0.05:
        _burst_active = True
        _burst_end = now + random.uniform(5.0, 30.0)

    if _burst_active:
        if now < _burst_end:
            multiplier *= 8.0
        else:
            _burst_active = False

    return multiplier
