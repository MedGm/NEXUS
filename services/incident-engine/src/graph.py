import logging
import threading
import uuid
from datetime import datetime, timezone

import networkx as nx

from .rules import apply_rules

log = logging.getLogger(__name__)

_SEVERITY_ORDER = {"HIGH": 0, "MEDIUM": 1, "LOW": 2}


def find_root_cause_node(graph: nx.DiGraph) -> str | None:
    """Returns node_id of root: in_degree==0, highest severity, earliest timestamp."""
    if graph.number_of_nodes() == 0:
        return None
    roots = [(nid, data) for nid, data in graph.nodes(data=True) if graph.in_degree(nid) == 0]
    if not roots:
        # All nodes have incoming edges — take earliest overall
        roots = list(graph.nodes(data=True))
    roots.sort(key=lambda x: (
        _SEVERITY_ORDER.get(x[1].get("severity", "LOW"), 2),
        x[1].get("timestamp", 0),
    ))
    return roots[0][0]


class CausalGraphBuilder:
    """
    Manages one incident's causal graph. Thread-safe via _lock.
    add_node() returns True the first time an incident is opened (>=2 connected nodes via a rule edge).
    """

    def __init__(self):
        self._graph: nx.DiGraph = nx.DiGraph()
        self._lock = threading.Lock()
        self.incident_id: str | None = None
        self.started_at: datetime | None = None
        self.last_event_at: datetime | None = None

    def add_node(self, node: dict) -> bool:
        """Add node + apply rules. Returns True if this call opened a new incident."""
        with self._lock:
            self._graph.add_node(node["node_id"], **node)
            new_edges = apply_rules(self._graph, node)
            for edge in new_edges:
                self._graph.add_edge(
                    edge["source_id"], edge["target_id"],
                    type=edge["type"], rule_id=edge["rule_id"],
                    confidence=edge["confidence"],
                )
            self.last_event_at = datetime.now(timezone.utc)

            if self.incident_id is None and self._graph.number_of_edges() >= 1:
                self.incident_id = str(uuid.uuid4())
                self.started_at = datetime.now(timezone.utc)
                log.info("Incident opened: %s (%d nodes)", self.incident_id, self._graph.number_of_nodes())
                return True
            return False

    def to_json(self) -> dict:
        with self._lock:
            nodes = [dict(data) for _, data in self._graph.nodes(data=True)]
            edges = [
                {"source_id": u, "target_id": v, **d}
                for u, v, d in self._graph.edges(data=True)
            ]
            return {"nodes": nodes, "edges": edges}

    def node_count(self) -> int:
        with self._lock:
            return self._graph.number_of_nodes()

    def max_severity(self) -> str:
        with self._lock:
            severities = [d.get("severity", "LOW") for _, d in self._graph.nodes(data=True)]
            if not severities:
                return "LOW"
            return min(severities, key=lambda s: _SEVERITY_ORDER.get(s, 2))

    def root_cause_node_id(self) -> str | None:
        with self._lock:
            return find_root_cause_node(self._graph)
