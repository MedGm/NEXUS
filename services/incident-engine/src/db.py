import os
from datetime import datetime, timezone

import psycopg2
import psycopg2.extras


def get_incidents_conn():
    return psycopg2.connect(os.environ["POSTGRES_INCIDENTS_DSN"])


def get_metrics_conn():
    return psycopg2.connect(os.environ["POSTGRES_METRICS_DSN"])


def get_meta_conn():
    return psycopg2.connect(os.environ["POSTGRES_META_DSN"])


def persist_incident(
    conn,
    incident_id: str,
    started_at: datetime,
    resolved_at: datetime | None,
    root_cause_node_id: str | None,
    causal_graph_json: dict,
    llm_summary: str | None,
    severity: str,
    node_count: int,
) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO incidents.incidents
                (id, started_at, resolved_at, root_cause_node_id,
                 causal_graph_json, llm_summary, severity, node_count, status)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, 'open')
            ON CONFLICT (id) DO NOTHING
            """,
            (
                incident_id, started_at, resolved_at, root_cause_node_id,
                psycopg2.extras.Json(causal_graph_json), llm_summary,
                severity, node_count,
            ),
        )
    conn.commit()


def update_llm_summary(conn, incident_id: str, llm_summary: str) -> None:
    with conn.cursor() as cur:
        cur.execute(
            "UPDATE incidents.incidents SET llm_summary = %s WHERE id = %s",
            (llm_summary, incident_id),
        )
    conn.commit()


def get_incident(conn, incident_id: str) -> dict | None:
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute("SELECT * FROM incidents.incidents WHERE id = %s", (incident_id,))
        row = cur.fetchone()
    return dict(row) if row else None


def list_incidents(conn, status: str | None = None, limit: int = 20) -> list[dict]:
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        if status:
            cur.execute(
                """
                SELECT id, started_at, resolved_at, severity, status, node_count, root_cause_node_id
                FROM incidents.incidents WHERE status = %s
                ORDER BY started_at DESC LIMIT %s
                """,
                (status, limit),
            )
        else:
            cur.execute(
                """
                SELECT id, started_at, resolved_at, severity, status, node_count, root_cause_node_id
                FROM incidents.incidents
                ORDER BY started_at DESC LIMIT %s
                """,
                (limit,),
            )
        return [dict(r) for r in cur.fetchall()]


def resolve_incident(conn, incident_id: str) -> bool:
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE incidents.incidents SET status = 'resolved', resolved_at = %s
            WHERE id = %s AND status = 'open'
            """,
            (datetime.now(timezone.utc), incident_id),
        )
        updated = cur.rowcount
    conn.commit()
    return updated > 0


def get_last_trust_scores(conn, dataset_id: str) -> list[float]:
    """Returns [latest, previous] composite_scores for TrustDrop delta."""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT composite_score FROM trust.dataset_trust_scores
            WHERE dataset_id = %s ORDER BY timestamp DESC LIMIT 2
            """,
            (dataset_id,),
        )
        return [float(r[0]) for r in cur.fetchall()]


def get_new_drift_flags(conn, since_id: int) -> list[dict]:
    """Returns model_drift_flags rows with id > since_id and status='flagged'."""
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(
            """
            SELECT id, event_type, ks_stat FROM models.model_drift_flags
            WHERE id > %s AND status = 'flagged' ORDER BY id ASC
            """,
            (since_id,),
        )
        return [dict(r) for r in cur.fetchall()]
