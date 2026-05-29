import json
import logging
import os
from datetime import timedelta

import boto3
import networkx as nx

from .taxonomy import anomaly_event_to_node
from .rules import apply_rules
from .graph import find_root_cause_node

log = logging.getLogger(__name__)

RAW_DATA_BUCKET = "raw-data"


def _minio_client():
    return boto3.client(
        "s3",
        endpoint_url=os.environ.get("MINIO_ENDPOINT", "http://minio:9000"),
        aws_access_key_id=os.environ.get("MINIO_ACCESS_KEY", "minioadmin"),
        aws_secret_access_key=os.environ.get("MINIO_SECRET_KEY", "minioadmin"),
    )


def _list_keys_for_window(s3, started_at, resolved_at) -> list[str]:
    """Return all raw-data/ keys whose hour path intersects the incident window."""
    paginator = s3.get_paginator("list_objects_v2")
    all_keys = []
    for page in paginator.paginate(Bucket=RAW_DATA_BUCKET):
        for obj in page.get("Contents", []):
            all_keys.append(obj["Key"])

    result = []
    dt = started_at.replace(minute=0, second=0, microsecond=0)
    end_dt = resolved_at if resolved_at else started_at
    while dt <= end_dt:
        prefix = (
            f"year={dt.year}/month={dt.month:02d}"
            f"/day={dt.day:02d}/hour={dt.hour:02d}/"
        )
        result.extend(k for k in all_keys if k.startswith(prefix))
        dt += timedelta(hours=1)
    return result


def _build_replay_graph(
    s3, keys: list[str], started_at_ms: int, resolved_at_ms: int
) -> dict:
    nodes_in_order = []
    for key in keys:
        try:
            obj = s3.get_object(Bucket=RAW_DATA_BUCKET, Key=key)
            for line in obj["Body"].iter_lines():
                try:
                    record = json.loads(line)
                    ts = record.get("timestamp", 0)
                    if ts < started_at_ms or ts > resolved_at_ms:
                        continue
                    node = anomaly_event_to_node({
                        "event_id": record.get("event_id", ""),
                        "event_type": record.get("event_type", ""),
                        "anomaly_score": 0.5,
                        "is_anomaly": True,
                        "threshold": 0.75,
                        "tier": "replay",
                        "timestamp": ts,
                    })
                    if node:
                        nodes_in_order.append(node)
                except Exception:
                    continue
        except Exception as e:
            log.warning("Failed to read MinIO key %s: %s", key, e)

    nodes_in_order.sort(key=lambda n: n["timestamp"])
    G = nx.DiGraph()
    for node in nodes_in_order:
        G.add_node(node["node_id"], **node)
        for edge in apply_rules(G, node):
            G.add_edge(
                edge["source_id"], edge["target_id"],
                type=edge["type"], rule_id=edge["rule_id"],
                confidence=edge["confidence"],
            )

    return {
        "nodes": [dict(d) for _, d in G.nodes(data=True)],
        "edges": [
            {"source_id": u, "target_id": v, **d}
            for u, v, d in G.edges(data=True)
        ],
    }


def replay_incident(incident: dict) -> dict:
    """
    Re-analyze past incident using current causal rules.
    Returns diff. No side effects (no Postgres write, no Kafka publish).
    """
    original_graph = incident["causal_graph_json"]
    started_at = incident["started_at"]
    resolved_at = incident.get("resolved_at") or started_at
    started_at_ms = int(started_at.timestamp() * 1000)
    resolved_at_ms = int(resolved_at.timestamp() * 1000)

    s3 = _minio_client()
    keys = _list_keys_for_window(s3, started_at, resolved_at)
    replay_graph = _build_replay_graph(s3, keys, started_at_ms, resolved_at_ms)

    # Diff by rule_id (node_ids differ between original and replay)
    original_rule_ids = {e["rule_id"] for e in original_graph.get("edges", [])}
    replay_rule_ids = {e["rule_id"] for e in replay_graph.get("edges", [])}
    added_edges = [e for e in replay_graph["edges"] if e["rule_id"] not in original_rule_ids]
    removed_edges = [e for e in original_graph.get("edges", []) if e["rule_id"] not in replay_rule_ids]

    # Root cause comparison by type (node_ids differ)
    def _root_type(graph_json: dict) -> str | None:
        G = nx.DiGraph()
        for n in graph_json.get("nodes", []):
            G.add_node(n["node_id"], **n)
        for e in graph_json.get("edges", []):
            G.add_edge(e["source_id"], e["target_id"])
        root_id = find_root_cause_node(G)
        return G.nodes[root_id].get("type") if root_id else None

    root_cause_changed = _root_type(original_graph) != _root_type(replay_graph)

    return {
        "original_graph": original_graph,
        "replay_graph": replay_graph,
        "added_edges": added_edges,
        "removed_edges": removed_edges,
        "root_cause_changed": root_cause_changed,
    }
