from datetime import datetime
from unittest.mock import patch
from src.temporal import rate_multiplier


def test_business_hours_weekday_returns_3x():
    dt = datetime(2026, 5, 27, 14, 0, 0)  # Wednesday 14:00
    with patch("src.temporal._burst_active", False):
        result = rate_multiplier(dt)
    assert result == 3.0


def test_weekend_returns_0_3x():
    dt = datetime(2026, 5, 30, 14, 0, 0)  # Saturday 14:00
    with patch("src.temporal._burst_active", False):
        result = rate_multiplier(dt)
    assert result == 0.3


def test_off_hours_weekday_returns_1x():
    dt = datetime(2026, 5, 27, 3, 0, 0)  # Wednesday 03:00
    with patch("src.temporal._burst_active", False):
        result = rate_multiplier(dt)
    assert result == 1.0


def test_burst_multiplies_current_rate():
    dt = datetime(2026, 5, 27, 3, 0, 0)  # off-hours: base ×1
    import time
    future = time.monotonic() + 30
    with patch("src.temporal._burst_active", True), \
         patch("src.temporal._burst_end", future):
        result = rate_multiplier(dt)
    assert result == 8.0


def test_business_hours_burst_composes():
    dt = datetime(2026, 5, 27, 14, 0, 0)  # business hours ×3
    import time
    future = time.monotonic() + 30
    with patch("src.temporal._burst_active", True), \
         patch("src.temporal._burst_end", future):
        result = rate_multiplier(dt)
    assert result == 24.0  # 3 × 8
