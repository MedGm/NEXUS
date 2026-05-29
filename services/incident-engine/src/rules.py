from dataclasses import dataclass

import networkx as nx


@dataclass(frozen=True)
class CausalRule:
    rule_id: str
    source_type: str
    target_type: str
    edge_type: str          # caused_by | preceded_by | correlated_with
    time_window_s: float
    same_dataset: bool


CAUSAL_RULES: list[CausalRule] = [
    CausalRule("R01", "SchemaChange",   "KafkaLag",        "caused_by",       300,  False),
    CausalRule("R02", "SchemaChange",   "ErrorRateSpike",  "caused_by",       600,  True),
    CausalRule("R03", "KafkaLag",       "TrustDrop",       "caused_by",       600,  False),
    CausalRule("R04", "ErrorRateSpike", "TrustDrop",       "caused_by",       900,  True),
    CausalRule("R05", "TrustDrop",      "PredictionDrift", "caused_by",       1800, True),
    CausalRule("R06", "Deploy",         "SchemaChange",    "preceded_by",     1800, False),
    CausalRule("R07", "Deploy",         "ErrorRateSpike",  "preceded_by",     3600, False),
    CausalRule("R08", "SchemaChange",   "PredictionDrift", "correlated_with", 3600, False),
    CausalRule("R09", "KafkaLag",       "ErrorRateSpike",  "correlated_with", 300,  False),
    CausalRule("R10", "ErrorRateSpike", "ErrorRateSpike",  "correlated_with", 300,  False),
]


def apply_rules(graph: nx.DiGraph, new_node: dict) -> list[dict]:
    """Check new_node against all existing nodes. Return list of edges to add."""
    edges = []
    for rule in CAUSAL_RULES:
        if new_node["type"] != rule.target_type:
            continue
        for src_id, src_data in graph.nodes(data=True):
            if src_data["type"] != rule.source_type:
                continue
            dt = (new_node["timestamp"] - src_data["timestamp"]) / 1000
            if not (0 <= dt <= rule.time_window_s):
                continue
            if rule.same_dataset and src_data.get("dataset_id") != new_node.get("dataset_id"):
                continue
            if rule.rule_id == "R10" and src_data.get("dataset_id") == new_node.get("dataset_id"):
                continue  # R10: different datasets only
            edges.append({
                "source_id": src_id,
                "target_id": new_node["node_id"],
                "type": rule.edge_type,
                "rule_id": rule.rule_id,
                "confidence": 0.9,
            })
    return edges
