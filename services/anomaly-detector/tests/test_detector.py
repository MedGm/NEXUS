import pytest
from src.detector import AnomalyDetector, make_model, THRESHOLDS


def test_order_placed_score_returns_float_and_bool():
    det = AnomalyDetector("OrderPlaced")
    score, is_anomaly = det.score_one({"total_usd": 100.0, "item_count": 2.0, "inter_arrival_ms": 300.0})
    assert isinstance(score, float)
    assert isinstance(is_anomaly, bool)
    assert 0.0 <= score <= 1.0


def test_score_before_learn_correct_order():
    det = AnomalyDetector("OrderPlaced")
    f = {"total_usd": 100.0, "item_count": 2.0, "inter_arrival_ms": 300.0}
    score1, _ = det.score_one(f)
    score2, _ = det.score_one(f)
    assert 0.0 <= score1 <= 1.0
    assert 0.0 <= score2 <= 1.0


def test_threshold_zero_makes_everything_anomalous():
    det = AnomalyDetector("OrderPlaced", threshold=0.0)
    f = {"total_usd": 100.0, "item_count": 2.0, "inter_arrival_ms": 300.0}
    for _ in range(260):
        det.score_one(f)
    _, is_anomaly = det.score_one(f)
    assert is_anomaly is True


def test_threshold_one_makes_nothing_anomalous():
    det = AnomalyDetector("PaymentProcessed", threshold=1.0)
    f = {"amount_usd": 50.0, "is_failure": 0.0, "consumer_lag_ms": 100.0}
    _, is_anomaly = det.score_one(f)
    assert is_anomaly is False


def test_price_snapshot_adwin_no_drift_on_stable_stream():
    det = AnomalyDetector("PriceSnapshot")
    for _ in range(50):
        _, is_anomaly = det.score_one({"pct_change_24h": 0.5, "volume_24h_usd": 1e10})
    assert is_anomaly is False


def test_price_snapshot_adwin_detects_sudden_shift():
    det = AnomalyDetector("PriceSnapshot")
    for _ in range(200):
        det.score_one({"pct_change_24h": 0.1, "volume_24h_usd": 1e10})
    drift_found = any(
        det.score_one({"pct_change_24h": 80.0, "volume_24h_usd": 1e10})[1]
        for _ in range(50)
    )
    assert drift_found


def test_all_hst_types_have_float_thresholds():
    for t in ["OrderPlaced", "PaymentProcessed", "SessionStarted", "RecommendationServed"]:
        assert isinstance(THRESHOLDS[t], float)


def test_price_snapshot_threshold_is_none():
    assert THRESHOLDS["PriceSnapshot"] is None


def test_model_property_accessible():
    det = AnomalyDetector("SessionStarted")
    assert det.model is not None
