import pytest
from src.taxonomy import (
    severity_for_error_rate_spike,
    severity_for_trust_drop,
    severity_for_kafka_lag,
    anomaly_event_to_node,
    node_from_event_payload,
    make_trust_drop_node,
    make_kafka_lag_node,
    make_prediction_drift_node,
)


def test_severity_error_high():
    assert severity_for_error_rate_spike(0.90) == "HIGH"

def test_severity_error_medium():
    assert severity_for_error_rate_spike(0.80) == "MEDIUM"

def test_severity_error_low():
    assert severity_for_error_rate_spike(0.60) == "LOW"

def test_severity_trust_drop_high():
    assert severity_for_trust_drop(0.35) == "HIGH"

def test_severity_trust_drop_medium():
    assert severity_for_trust_drop(0.20) == "MEDIUM"

def test_severity_trust_drop_low():
    assert severity_for_trust_drop(0.10) == "LOW"

def test_severity_kafka_lag_high():
    assert severity_for_kafka_lag(15000) == "HIGH"

def test_severity_kafka_lag_medium():
    assert severity_for_kafka_lag(7000) == "MEDIUM"

def test_severity_kafka_lag_low():
    assert severity_for_kafka_lag(100) == "LOW"

def test_anomaly_event_to_node_returns_none_if_not_anomaly():
    event = {"event_id": "x", "event_type": "OrderPlaced", "anomaly_score": 0.5,
             "is_anomaly": False, "threshold": 0.75, "tier": "fallback", "timestamp": 1000}
    assert anomaly_event_to_node(event) is None

def test_anomaly_event_to_node_returns_node_if_anomaly():
    event = {"event_id": "x", "event_type": "OrderPlaced", "anomaly_score": 0.90,
             "is_anomaly": True, "threshold": 0.75, "tier": "accurate", "timestamp": 1000}
    node = anomaly_event_to_node(event)
    assert node is not None
    assert node["type"] == "ErrorRateSpike"
    assert node["dataset_id"] == "OrderPlaced"
    assert node["severity"] == "HIGH"
    assert node["timestamp"] == 1000
    assert "node_id" in node
    assert "metadata" in node

def test_anomaly_event_to_node_medium_severity():
    event = {"event_id": "x", "event_type": "PaymentProcessed", "anomaly_score": 0.78,
             "is_anomaly": True, "threshold": 0.75, "tier": "accurate", "timestamp": 2000}
    node = anomaly_event_to_node(event)
    assert node["severity"] == "MEDIUM"

def test_node_from_event_payload_deploy():
    payload = {"type": "Deploy", "dataset_id": "all", "metadata": {"version": "1.2.3"}, "severity": "MEDIUM"}
    node = node_from_event_payload(payload)
    assert node["type"] == "Deploy"
    assert node["severity"] == "MEDIUM"
    assert node["dataset_id"] == "all"

def test_node_from_event_payload_rejects_invalid_type():
    with pytest.raises(ValueError):
        node_from_event_payload({"type": "ErrorRateSpike", "dataset_id": "all", "metadata": {}})

def test_make_trust_drop_node_structure():
    node = make_trust_drop_node("OrderPlaced", 0.20, 0.90, 0.70)
    assert node["type"] == "TrustDrop"
    assert node["dataset_id"] == "OrderPlaced"
    assert node["severity"] == "MEDIUM"
    assert node["metadata"]["drop"] == 0.20

def test_make_kafka_lag_node_structure():
    node = make_kafka_lag_node(12000)
    assert node["type"] == "KafkaLag"
    assert node["severity"] == "HIGH"

def test_make_prediction_drift_node_structure():
    node = make_prediction_drift_node("OrderPlaced", 0.22, 7)
    assert node["type"] == "PredictionDrift"
    assert node["severity"] == "MEDIUM"
    assert node["metadata"]["drift_flag_id"] == 7
