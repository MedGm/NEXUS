import time
import uuid

import networkx as nx
import pytest

from src.rules import CAUSAL_RULES, apply_rules


def _make_node(node_type, dataset_id, ts_offset_s=0, severity="MEDIUM"):
    now_ms = int(time.time() * 1000)
    return {
        "node_id": str(uuid.uuid4()),
        "type": node_type,
        "dataset_id": dataset_id,
        "timestamp": now_ms + int(ts_offset_s * 1000),
        "severity": severity,
        "metadata": {},
    }


def _graph_with(*nodes):
    G = nx.DiGraph()
    for n in nodes:
        G.add_node(n["node_id"], **n)
    return G


def test_all_rules_present():
    assert len(CAUSAL_RULES) == 10


def test_all_rule_ids_unique():
    ids = [r.rule_id for r in CAUSAL_RULES]
    assert len(set(ids)) == 10


def test_all_edge_types_valid():
    valid = {"caused_by", "preceded_by", "correlated_with"}
    for rule in CAUSAL_RULES:
        assert rule.edge_type in valid


def test_r01_schema_change_causes_kafka_lag_within_window():
    src = _make_node("SchemaChange", "A", ts_offset_s=-120)
    G = _graph_with(src)
    new = _make_node("KafkaLag", "B")
    edges = apply_rules(G, new)
    assert any(e["rule_id"] == "R01" for e in edges)


def test_r01_no_match_outside_window():
    src = _make_node("SchemaChange", "A", ts_offset_s=-400)  # > 5 min
    G = _graph_with(src)
    new = _make_node("KafkaLag", "B")
    edges = apply_rules(G, new)
    assert not any(e["rule_id"] == "R01" for e in edges)


def test_r02_requires_same_dataset():
    src = _make_node("SchemaChange", "OrderPlaced", ts_offset_s=-300)
    G = _graph_with(src)
    # same dataset → fires
    new_same = _make_node("ErrorRateSpike", "OrderPlaced")
    assert any(e["rule_id"] == "R02" for e in apply_rules(G, new_same))
    # different dataset → no fire
    new_diff = _make_node("ErrorRateSpike", "PaymentProcessed")
    assert not any(e["rule_id"] == "R02" for e in apply_rules(G, new_diff))


def test_r05_trust_drop_to_prediction_drift_same_dataset():
    src = _make_node("TrustDrop", "OrderPlaced", ts_offset_s=-600)
    G = _graph_with(src)
    new = _make_node("PredictionDrift", "OrderPlaced")
    assert any(e["rule_id"] == "R05" for e in apply_rules(G, new))


def test_r10_correlated_error_spikes_different_datasets():
    src = _make_node("ErrorRateSpike", "OrderPlaced", ts_offset_s=-60)
    G = _graph_with(src)
    new = _make_node("ErrorRateSpike", "PaymentProcessed")
    assert any(e["rule_id"] == "R10" for e in apply_rules(G, new))


def test_r10_no_match_same_dataset():
    src = _make_node("ErrorRateSpike", "OrderPlaced", ts_offset_s=-60)
    G = _graph_with(src)
    new = _make_node("ErrorRateSpike", "OrderPlaced")
    assert not any(e["rule_id"] == "R10" for e in apply_rules(G, new))


def test_no_future_edges():
    """Node in the future cannot be a source for a node in the present."""
    src = _make_node("SchemaChange", "A", ts_offset_s=+60)  # future
    G = _graph_with(src)
    new = _make_node("KafkaLag", "B")  # now
    edges = apply_rules(G, new)
    assert not any(e["rule_id"] == "R01" for e in edges)


def test_edges_have_required_fields():
    src = _make_node("SchemaChange", "A", ts_offset_s=-120)
    G = _graph_with(src)
    new = _make_node("KafkaLag", "B")
    edges = apply_rules(G, new)
    for e in edges:
        assert "source_id" in e
        assert "target_id" in e
        assert "type" in e
        assert "rule_id" in e
        assert "confidence" in e
