from src.fallback import StatisticalFallback


def _f(val: float) -> dict[str, float]:
    return {"total_usd": val, "item_count": 2.0}


def test_returns_zero_during_warmup():
    fb = StatisticalFallback(warmup_events=20)
    score = fb.score("OrderPlaced", _f(100.0))
    assert score == 0.0


def test_warmed_after_n_events():
    fb = StatisticalFallback(warmup_events=10)
    for i in range(10):
        fb.score("OrderPlaced", _f(50.0 + i))
    score = fb.score("OrderPlaced", _f(55.0))
    assert score == 0.0


def test_extreme_outlier_flagged():
    fb = StatisticalFallback(warmup_events=20)
    for i in range(20):
        fb.score("OrderPlaced", _f(100.0 + i))
    score = fb.score("OrderPlaced", _f(1_000_000.0))
    assert score == 1.0


def test_datasets_are_independent():
    fb = StatisticalFallback(warmup_events=10)
    for _ in range(10):
        fb.score("OrderPlaced", _f(100.0))
        fb.score("PaymentProcessed", {"amount_usd": 50.0, "is_failure": 0.0, "consumer_lag_ms": 100.0})
    s1 = fb.score("OrderPlaced", _f(100.0))
    s2 = fb.score("PaymentProcessed", {"amount_usd": 50.0, "is_failure": 0.0, "consumer_lag_ms": 100.0})
    assert s1 == 0.0
    assert s2 == 0.0


def test_degenerate_all_identical_values():
    fb = StatisticalFallback(warmup_events=10)
    for _ in range(10):
        fb.score("OrderPlaced", {"total_usd": 100.0, "item_count": 2.0})
    score = fb.score("OrderPlaced", {"total_usd": 100.1, "item_count": 2.0})
    assert score == 1.0
