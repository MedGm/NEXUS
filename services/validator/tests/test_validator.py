import os
import pytest
from src.validator import GEValidator

EXPECTATIONS_DIR = os.path.join(os.path.dirname(__file__), "..", "expectations")


@pytest.fixture
def validator():
    return GEValidator(EXPECTATIONS_DIR)


def _order_record(**overrides):
    base = {
        "event_id": "test-id",
        "timestamp": 1779892200000,
        "order_id": "ord-001",
        "user_id": "usr-001",
        "items": [],
        "total_usd": 100.0,
        "currency": "USD",
        "country": "US",
    }
    base.update(overrides)
    return base


def test_valid_order_placed_passes(validator):
    result = validator.validate("OrderPlaced", [_order_record()])
    assert result["success"] is True
    assert result["null_rate"] == 0.0
    assert "psi_values" in result["results_json"]
    assert "total_usd" in result["results_json"]["psi_values"]


def test_null_order_id_fails(validator):
    result = validator.validate("OrderPlaced", [_order_record(order_id=None)])
    assert result["success"] is False


def test_null_rate_counts_nulls_correctly(validator):
    # 1 null field (order_id) out of 8 fields * 1 event
    result = validator.validate("OrderPlaced", [_order_record(order_id=None)])
    assert result["null_rate"] > 0.0
    assert result["null_rate"] < 1.0


def test_psi_values_extracted_for_psi_columns(validator):
    records = [_order_record(total_usd=float(i * 10)) for i in range(1, 6)]
    result = validator.validate("OrderPlaced", records)
    psi_vals = result["results_json"]["psi_values"]
    assert "total_usd" in psi_vals
    assert len(psi_vals["total_usd"]) == 5


def test_batch_size_reflected_in_result(validator):
    records = [_order_record() for _ in range(3)]
    result = validator.validate("OrderPlaced", records)
    assert result["batch_size"] == 3


def test_negative_total_usd_fails(validator):
    result = validator.validate("OrderPlaced", [_order_record(total_usd=-1.0)])
    assert result["success"] is False


def test_price_snapshot_valid(validator):
    record = {
        "event_id": "x",
        "timestamp": 1779892200000,
        "symbol": "bitcoin",
        "price_usd": 50000.0,
        "market_cap_usd": 1e12,
        "volume_24h_usd": 3e10,
        "pct_change_24h": 2.5,
        "sampled_at": "2026-05-28T00:00:00Z",
    }
    result = validator.validate("PriceSnapshot", [record])
    assert result["success"] is True
