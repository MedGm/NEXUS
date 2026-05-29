import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import pytest
from nexus.dag_helpers import compute_ks, detect_event_type, extract_features_for_type


def test_identical_distributions_near_zero():
    data = [float(i % 10) for i in range(200)]
    stat, pval = compute_ks(data[:100], data[100:])
    assert stat < 0.1
    assert pval > 0.05


def test_very_different_distributions_high_stat():
    ref = [1.0] * 100
    obs = [10.0] * 100
    stat, pval = compute_ks(ref, obs)
    assert stat > 0.5


def test_returns_floats():
    stat, pval = compute_ks([0.1, 0.2] * 50, [0.3, 0.4] * 50)
    assert isinstance(stat, float)
    assert isinstance(pval, float)


def test_detects_order_placed():
    r = {"order_id": "x", "total_usd": 100.0, "user_id": "u"}
    assert detect_event_type(r) == "OrderPlaced"


def test_detects_payment_processed():
    r = {"payment_id": "p", "amount_usd": 50.0, "status": "success"}
    assert detect_event_type(r) == "PaymentProcessed"


def test_detects_session_started():
    r = {"session_id": "s", "user_id": "u", "device": "mobile"}
    assert detect_event_type(r) == "SessionStarted"


def test_detects_recommendation_served():
    r = {"rec_id": "r", "user_id": "u", "score": 0.8}
    assert detect_event_type(r) == "RecommendationServed"


def test_detects_price_snapshot():
    r = {"symbol": "bitcoin", "price_usd": 50000.0, "pct_change_24h": 2.5}
    assert detect_event_type(r) == "PriceSnapshot"


def test_returns_none_for_unknown():
    assert detect_event_type({"foo": "bar"}) is None


def test_extract_order_placed():
    import time
    ts = int(time.time() * 1000)
    records = [{"event_id": "x", "timestamp": ts, "order_id": "o",
                "user_id": "u", "items": [{"sku": "A"}], "total_usd": 100.0,
                "currency": "USD", "country": "US"}]
    feats = extract_features_for_type("OrderPlaced", records)
    assert len(feats) == 1
    assert set(feats[0].keys()) == {"total_usd", "item_count", "inter_arrival_ms"}


def test_extract_filters_wrong_type():
    import time
    ts = int(time.time() * 1000)
    records = [
        {"event_id": "x", "timestamp": ts, "session_id": "s",
         "user_id": "u", "device": "mobile", "os": "iOS", "browser": "Safari",
         "ip": "1.1.1.1", "referrer": ""},
    ]
    feats = extract_features_for_type("OrderPlaced", records)
    assert feats == []
