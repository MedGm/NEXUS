import pytest
from src.model_store import ModelRegistry, SCORE_BUFFER_SIZE


def _make_registry_no_mlflow():
    """Bypass __init__ to avoid MLflow connection."""
    from src.detector import AnomalyDetector
    reg = ModelRegistry.__new__(ModelRegistry)
    reg._detectors = {
        t: AnomalyDetector(t)
        for t in ["OrderPlaced", "PaymentProcessed", "SessionStarted",
                  "RecommendationServed", "PriceSnapshot"]
    }
    reg._counts = {t: 0 for t in reg._detectors}
    reg._score_buffers = {t: [] for t in reg._detectors}
    return reg


def test_score_buffer_initializes_empty():
    reg = _make_registry_no_mlflow()
    assert reg._score_buffers["OrderPlaced"] == []


def test_score_buffer_accumulates_after_score_and_learn():
    reg = _make_registry_no_mlflow()
    reg._checkpoint = lambda et: None  # skip MLflow

    for _ in range(5):
        reg.score_and_learn(
            "OrderPlaced",
            {"total_usd": 100.0, "item_count": 2.0, "inter_arrival_ms": 300.0},
        )
    assert len(reg._score_buffers["OrderPlaced"]) == 5


def test_score_buffer_capped_at_buffer_size():
    reg = _make_registry_no_mlflow()
    reg._checkpoint = lambda et: None

    for _ in range(SCORE_BUFFER_SIZE + 10):
        reg.score_and_learn(
            "SessionStarted",
            {"inter_arrival_ms": 100.0, "is_new_user": 0.0},
        )
    assert len(reg._score_buffers["SessionStarted"]) == SCORE_BUFFER_SIZE


def test_get_score_distribution_structure():
    reg = _make_registry_no_mlflow()
    reg._score_buffers["OrderPlaced"] = [0.1, 0.2, 0.3]
    reg._counts["OrderPlaced"] = 42

    dist = reg._get_score_distribution("OrderPlaced")
    assert dist["scores"] == [0.1, 0.2, 0.3]
    assert dist["n_samples"] == 3
    assert dist["event_type"] == "OrderPlaced"
    assert "captured_at_ms" in dist
    assert isinstance(dist["captured_at_ms"], int)


def test_score_buffers_are_independent_per_type():
    reg = _make_registry_no_mlflow()
    reg._checkpoint = lambda et: None

    reg.score_and_learn("OrderPlaced",
        {"total_usd": 100.0, "item_count": 2.0, "inter_arrival_ms": 300.0})
    assert len(reg._score_buffers["OrderPlaced"]) == 1
    assert len(reg._score_buffers["PaymentProcessed"]) == 0
