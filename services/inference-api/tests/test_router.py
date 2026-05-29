import pytest
from src.router import TwoTierRouter


def test_accurate_high_trust_low_lag():
    r = TwoTierRouter(trust_gate=0.6, lag_threshold_ms=5000)
    tier, reason = r.route("OrderPlaced", {"consumer_lag_ms": 100.0}, trust_score=0.9)
    assert tier == "accurate"
    assert reason is None


def test_fallback_high_lag():
    r = TwoTierRouter(trust_gate=0.6, lag_threshold_ms=5000)
    tier, reason = r.route("OrderPlaced", {"consumer_lag_ms": 6000.0}, trust_score=0.9)
    assert tier == "fallback"
    assert reason is None


def test_reject_low_trust():
    r = TwoTierRouter(trust_gate=0.6, lag_threshold_ms=5000)
    tier, reason = r.route("OrderPlaced", {"consumer_lag_ms": 100.0}, trust_score=0.5)
    assert tier == "reject"
    assert "0.50" in reason
    assert "OrderPlaced" in reason


def test_at_trust_threshold_passes():
    r = TwoTierRouter(trust_gate=0.6, lag_threshold_ms=5000)
    tier, _ = r.route("OrderPlaced", {}, trust_score=0.6)
    assert tier == "accurate"


def test_just_below_trust_threshold_rejects():
    r = TwoTierRouter(trust_gate=0.6, lag_threshold_ms=5000)
    tier, _ = r.route("OrderPlaced", {}, trust_score=0.5999)
    assert tier == "reject"


def test_at_lag_threshold_uses_fallback():
    r = TwoTierRouter(trust_gate=0.6, lag_threshold_ms=5000)
    tier, _ = r.route("OrderPlaced", {"consumer_lag_ms": 5000.0}, trust_score=0.9)
    assert tier == "fallback"


def test_no_trust_score_uses_fallback():
    r = TwoTierRouter(trust_gate=0.6, lag_threshold_ms=5000)
    tier, reason = r.route("OrderPlaced", {}, trust_score=None)
    assert tier == "fallback"
    assert reason is None


def test_no_lag_in_features_defaults_to_accurate():
    r = TwoTierRouter(trust_gate=0.6, lag_threshold_ms=5000)
    tier, _ = r.route("OrderPlaced", {"total_usd": 100.0}, trust_score=0.9)
    assert tier == "accurate"
