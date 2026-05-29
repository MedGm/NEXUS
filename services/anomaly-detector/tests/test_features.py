import time
import pytest
from src.features import FeatureExtractor


def _ts():
    return int(time.time() * 1000)


def _order(total_usd=100.0, items=None, ts=None):
    return {
        "event_id": "x", "timestamp": ts or _ts(),
        "order_id": "o1", "user_id": "u1",
        "items": items or [{"sku": "A", "name": "X", "quantity": 1, "price_usd": 1.0}],
        "total_usd": total_usd, "currency": "USD", "country": "US",
    }


def _payment(amount=50.0, status="success"):
    return {
        "event_id": "x", "timestamp": _ts(), "payment_id": "p1",
        "order_id": "o1", "method": "card", "status": status,
        "amount_usd": amount, "gateway": "stripe",
    }


def _session(user_id="u1"):
    return {
        "event_id": "x", "timestamp": _ts(), "session_id": "s1",
        "user_id": user_id, "device": "mobile", "os": "iOS",
        "browser": "Safari", "ip": "1.1.1.1", "referrer": "",
    }


def _rec(score=0.8):
    return {
        "event_id": "x", "timestamp": _ts(), "rec_id": "r1",
        "user_id": "u1", "model_version": "v1", "items": ["a"],
        "score": score, "context": "home",
    }


def _price(pct=2.5, vol=3e10):
    return {
        "event_id": "x", "timestamp": _ts(), "symbol": "bitcoin",
        "price_usd": 50000.0, "market_cap_usd": 1e12,
        "volume_24h_usd": vol, "pct_change_24h": pct,
        "sampled_at": "2026-05-29T00:00:00Z",
    }


def test_order_feature_keys():
    fe = FeatureExtractor()
    f = fe.extract("OrderPlaced", _order())
    assert set(f.keys()) == {"total_usd", "item_count", "inter_arrival_ms"}


def test_order_total_usd():
    fe = FeatureExtractor()
    f = fe.extract("OrderPlaced", _order(total_usd=249.99))
    assert f["total_usd"] == pytest.approx(249.99)


def test_order_item_count():
    fe = FeatureExtractor()
    items = [{"sku": "A", "name": "X", "quantity": 1, "price_usd": 1.0}] * 4
    f = fe.extract("OrderPlaced", _order(items=items))
    assert f["item_count"] == 4.0


def test_inter_arrival_zero_on_first_event():
    fe = FeatureExtractor()
    f = fe.extract("OrderPlaced", _order())
    assert f["inter_arrival_ms"] == 0.0


def test_inter_arrival_positive_on_second_event():
    fe = FeatureExtractor()
    ts1, ts2 = 1_000_000_000_000, 1_000_000_000_500
    fe.extract("OrderPlaced", _order(ts=ts1))
    f = fe.extract("OrderPlaced", _order(ts=ts2))
    assert f["inter_arrival_ms"] == 500.0


def test_payment_is_failure_success():
    fe = FeatureExtractor()
    f = fe.extract("PaymentProcessed", _payment(status="success"))
    assert f["is_failure"] == 0.0


def test_payment_is_failure_failed():
    fe = FeatureExtractor()
    f = fe.extract("PaymentProcessed", _payment(status="declined"))
    assert f["is_failure"] == 1.0


def test_session_new_user_first_time():
    fe = FeatureExtractor()
    f = fe.extract("SessionStarted", _session(user_id="brand_new"))
    assert f["is_new_user"] == 1.0


def test_session_not_new_user_on_repeat():
    fe = FeatureExtractor()
    fe.extract("SessionStarted", _session(user_id="repeat_user"))
    f = fe.extract("SessionStarted", _session(user_id="repeat_user"))
    assert f["is_new_user"] == 0.0


def test_recommendation_feature_keys():
    fe = FeatureExtractor()
    f = fe.extract("RecommendationServed", _rec())
    assert set(f.keys()) == {"confidence_score", "consumer_lag_ms", "inter_arrival_ms"}


def test_recommendation_confidence_score():
    fe = FeatureExtractor()
    f = fe.extract("RecommendationServed", _rec(score=0.73))
    assert f["confidence_score"] == pytest.approx(0.73)


def test_price_snapshot_feature_keys():
    fe = FeatureExtractor()
    f = fe.extract("PriceSnapshot", _price())
    assert set(f.keys()) == {"pct_change_24h", "volume_24h_usd"}


def test_price_snapshot_values():
    fe = FeatureExtractor()
    f = fe.extract("PriceSnapshot", _price(pct=5.5, vol=2e10))
    assert f["pct_change_24h"] == pytest.approx(5.5)
    assert f["volume_24h_usd"] == pytest.approx(2e10)


def test_unknown_event_type_raises():
    fe = FeatureExtractor()
    with pytest.raises(ValueError, match="Unknown event_type"):
        fe.extract("UnknownType", {"event_id": "x"})
