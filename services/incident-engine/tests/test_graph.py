import time
import uuid

import networkx as nx

from src.graph import CausalGraphBuilder, find_root_cause_node


def _ts(offset_s=0):
    return int(time.time() * 1000) + int(offset_s * 1000)


def _make_node(node_type, dataset_id, ts_offset_s=0, severity="MEDIUM"):
    return {
        "node_id": str(uuid.uuid4()),
        "type": node_type,
        "dataset_id": dataset_id,
        "timestamp": _ts(ts_offset_s),
        "severity": severity,
        "metadata": {},
    }


def test_single_node_no_incident():
    builder = CausalGraphBuilder()
    node = _make_node("ErrorRateSpike", "OrderPlaced")
    opened = builder.add_node(node)
    assert opened is False
    assert builder.incident_id is None


def test_two_connected_nodes_opens_incident():
    builder = CausalGraphBuilder()
    src = _make_node("SchemaChange", "OrderPlaced", ts_offset_s=-120)
    builder.add_node(src)
    dst = _make_node("ErrorRateSpike", "OrderPlaced")
    opened = builder.add_node(dst)
    assert opened is True
    assert builder.incident_id is not None


def test_incident_id_stable_after_third_node():
    builder = CausalGraphBuilder()
    n1 = _make_node("SchemaChange", "OrderPlaced", ts_offset_s=-120)
    builder.add_node(n1)
    n2 = _make_node("ErrorRateSpike", "OrderPlaced")
    builder.add_node(n2)
    inc_id = builder.incident_id
    n3 = _make_node("TrustDrop", "OrderPlaced", ts_offset_s=+5)
    builder.add_node(n3)
    assert builder.incident_id == inc_id  # same incident, not a new one


def test_find_root_cause_in_degree_zero_earliest():
    G = nx.DiGraph()
    n1 = _make_node("SchemaChange", "A", ts_offset_s=-300)
    n2 = _make_node("ErrorRateSpike", "A", ts_offset_s=-180)
    G.add_node(n1["node_id"], **n1)
    G.add_node(n2["node_id"], **n2)
    G.add_edge(n1["node_id"], n2["node_id"])
    assert find_root_cause_node(G) == n1["node_id"]


def test_find_root_cause_multiple_roots_prefers_high_severity():
    G = nx.DiGraph()
    n1 = _make_node("Deploy", "X", ts_offset_s=-500, severity="HIGH")
    n2 = _make_node("SchemaChange", "Y", ts_offset_s=-400, severity="MEDIUM")
    G.add_node(n1["node_id"], **n1)
    G.add_node(n2["node_id"], **n2)
    # both in_degree==0, n1 has higher severity
    assert find_root_cause_node(G) == n1["node_id"]


def test_find_root_cause_empty_graph_returns_none():
    G = nx.DiGraph()
    assert find_root_cause_node(G) is None


def test_to_json_structure():
    builder = CausalGraphBuilder()
    src = _make_node("SchemaChange", "A", ts_offset_s=-120)
    builder.add_node(src)
    dst = _make_node("ErrorRateSpike", "A")
    builder.add_node(dst)
    j = builder.to_json()
    assert "nodes" in j
    assert "edges" in j
    assert len(j["nodes"]) == 2
    assert len(j["edges"]) >= 1  # R02 should fire


def test_max_severity_returns_highest():
    builder = CausalGraphBuilder()
    builder.add_node(_make_node("Deploy", "X", severity="MEDIUM", ts_offset_s=-1000))
    builder.add_node(_make_node("ErrorRateSpike", "X", severity="HIGH"))
    assert builder.max_severity() == "HIGH"
