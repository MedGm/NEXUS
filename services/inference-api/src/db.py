import os
from datetime import datetime

import psycopg2
import psycopg2.extras


def get_metrics_conn():
    return psycopg2.connect(os.environ["POSTGRES_DSN"])


def get_meta_conn():
    return psycopg2.connect(os.environ["POSTGRES_META_DSN"])


def get_trust_score(conn, dataset_id: str) -> float | None:
    """Returns latest composite_score for dataset_id, or None if not found."""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT composite_score
            FROM trust.dataset_trust_scores
            WHERE dataset_id = %s
            ORDER BY timestamp DESC
            LIMIT 1
            """,
            (dataset_id,),
        )
        row = cur.fetchone()
    return float(row[0]) if row else None


def write_inference_log(
    conn,
    dataset_id: str,
    event_id: str,
    model_name: str,
    model_version: int | None,
    anomaly_score: float,
    is_anomaly: bool,
    tier: str,
    trust_score_at_inference: float,
    consumer_lag_ms: int | None,
    timestamp: datetime,
) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO models.inference_log
                (dataset_id, event_id, model_name, model_version, anomaly_score,
                 is_anomaly, tier, trust_score_at_inference, consumer_lag_ms, timestamp)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                dataset_id, event_id, model_name, model_version, anomaly_score,
                is_anomaly, tier, trust_score_at_inference, consumer_lag_ms, timestamp,
            ),
        )
    conn.commit()


def get_active_shadow_flag_id(conn, dataset_id: str) -> int | None:
    """Returns the id of the active shadow drift flag for this dataset, or None."""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT id FROM models.model_drift_flags
            WHERE event_type = %s AND status = 'shadow'
            ORDER BY detected_at DESC LIMIT 1
            """,
            (dataset_id,),
        )
        row = cur.fetchone()
    return row[0] if row else None


def write_shadow_log(
    conn,
    drift_flag_id: int,
    event_id: str,
    dataset_id: str,
    production_score: float,
    shadow_score: float,
    shadow_model_version: int,
    shadow_latency_ms: int | None,
    timestamp,
) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO models.shadow_log
                (drift_flag_id, event_id, dataset_id, production_score, shadow_score,
                 shadow_model_version, shadow_latency_ms, timestamp)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (drift_flag_id, event_id, dataset_id, production_score, shadow_score,
             shadow_model_version, shadow_latency_ms, timestamp),
        )
    conn.commit()
