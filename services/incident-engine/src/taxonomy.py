import time
import uuid


NODE_TYPES = frozenset([
    "ErrorRateSpike", "TrustDrop", "PredictionDrift",
    "KafkaLag", "SchemaChange", "Deploy",
])


def severity_for_error_rate_spike(score: float) -> str:
    if score > 0.85:
        return "HIGH"
    if score > 0.75:
        return "MEDIUM"
    return "LOW"


def severity_for_trust_drop(drop: float) -> str:
    if drop > 0.30:
        return "HIGH"
    if drop > 0.15:
        return "MEDIUM"
    return "LOW"


def severity_for_kafka_lag(lag: int) -> str:
    if lag > 10000:
        return "HIGH"
    if lag > 5000:
        return "MEDIUM"
    return "LOW"


def anomaly_event_to_node(event: dict) -> dict | None:
    """Convert nexus.anomalies JSON message to graph node. Returns None if not anomaly."""
    if not event.get("is_anomaly"):
        return None
    score = float(event.get("anomaly_score", 0.0))
    return {
        "node_id": str(uuid.uuid4()),
        "type": "ErrorRateSpike",
        "dataset_id": event["event_type"],
        "timestamp": event["timestamp"],
        "severity": severity_for_error_rate_spike(score),
        "metadata": {
            "anomaly_score": score,
            "threshold": event.get("threshold"),
            "tier": event.get("tier"),
            "source_event_id": event.get("event_id"),
        },
    }


def node_from_event_payload(payload: dict) -> dict:
    """Create Deploy or SchemaChange node from POST /events payload."""
    node_type = payload.get("type", "")
    if node_type not in ("Deploy", "SchemaChange"):
        raise ValueError(
            f"Manual injection only supports Deploy/SchemaChange, got {node_type!r}"
        )
    return {
        "node_id": str(uuid.uuid4()),
        "type": node_type,
        "dataset_id": payload.get("dataset_id", "all"),
        "timestamp": int(time.time() * 1000),
        "severity": payload.get("severity", "MEDIUM"),
        "metadata": payload.get("metadata", {}),
    }


def make_trust_drop_node(dataset_id: str, drop: float, prev_score: float, curr_score: float) -> dict:
    return {
        "node_id": str(uuid.uuid4()),
        "type": "TrustDrop",
        "dataset_id": dataset_id,
        "timestamp": int(time.time() * 1000),
        "severity": severity_for_trust_drop(drop),
        "metadata": {"prev_score": prev_score, "curr_score": curr_score, "drop": drop},
    }


def make_kafka_lag_node(lag: int) -> dict:
    return {
        "node_id": str(uuid.uuid4()),
        "type": "KafkaLag",
        "dataset_id": "nexus-anomaly-detector",
        "timestamp": int(time.time() * 1000),
        "severity": severity_for_kafka_lag(lag),
        "metadata": {"lag": lag},
    }


def make_prediction_drift_node(event_type: str, ks_stat: float, flag_id: int) -> dict:
    return {
        "node_id": str(uuid.uuid4()),
        "type": "PredictionDrift",
        "dataset_id": event_type,
        "timestamp": int(time.time() * 1000),
        "severity": "MEDIUM",
        "metadata": {"ks_stat": ks_stat, "drift_flag_id": flag_id},
    }
