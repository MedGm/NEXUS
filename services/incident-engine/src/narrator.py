import logging
import os

import openai

log = logging.getLogger(__name__)

_client: openai.OpenAI | None = None


def _get_client() -> openai.OpenAI:
    global _client
    if _client is None:
        _client = openai.OpenAI(api_key=os.environ["OPENAI_API_KEY"])
    return _client


def generate_summary(graph_json: dict, incident_meta: dict) -> str | None:
    """Call OpenAI gpt-4o. Returns 3-sentence narrative or None on failure."""
    node_type_map = {n["node_id"]: n["type"] for n in graph_json.get("nodes", [])}
    nodes_text = "\n".join(
        f"- [{n['type']}] {n['dataset_id']} at {n['timestamp']} (severity={n['severity']})"
        for n in graph_json.get("nodes", [])
    )
    edges_text = "\n".join(
        f"- {node_type_map.get(e['source_id'], '?')} --[{e['type']}]--> "
        f"{node_type_map.get(e['target_id'], '?')} (rule {e['rule_id']})"
        for e in graph_json.get("edges", [])
    ) or "(no causal edges)"

    prompt = f"""You are an incident analyst for a real-time ML data platform.

Causal chain detected:
{nodes_text}

Causal edges:
{edges_text}

Root cause node ID: {incident_meta.get('root_cause_node_id', 'unknown')}
Severity: {incident_meta.get('severity', 'UNKNOWN')}
Duration: {incident_meta.get('duration_seconds', 0)}s

In exactly 3 sentences: (1) what triggered this incident, (2) what it propagated to, (3) what should be investigated first."""

    try:
        response = _get_client().chat.completions.create(
            model="gpt-4o",
            messages=[{"role": "user", "content": prompt}],
            max_tokens=300,
            temperature=0.2,
        )
        return response.choices[0].message.content
    except Exception as e:
        log.warning("LLM summary failed: %s", e)
        return None
