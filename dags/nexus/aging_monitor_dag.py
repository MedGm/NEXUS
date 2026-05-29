import json
import logging
import os
from datetime import datetime, timedelta, timezone

from pendulum import datetime as pendulum_datetime

import mlflow
import psycopg2
from airflow.decorators import dag, task
from mlflow.tracking import MlflowClient

try:
    from .dag_helpers import compute_ks, KS_THRESHOLD, MIN_SAMPLES
except ImportError:
    import sys, os
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
    from nexus.dag_helpers import compute_ks, KS_THRESHOLD, MIN_SAMPLES

log = logging.getLogger(__name__)

POSTGRES_META_DSN = os.environ.get(
    "POSTGRES_META_DSN", "postgresql://postgres:postgres@postgres/nexus_meta"
)
MLFLOW_TRACKING_URI = os.environ.get("MLFLOW_TRACKING_URI", "http://mlflow:5000")

EVENT_TYPES = [
    "OrderPlaced", "PaymentProcessed", "SessionStarted",
    "RecommendationServed", "PriceSnapshot",
]


def _load_reference_scores(client: MlflowClient, model_name: str) -> list[float] | None:
    try:
        versions = client.get_latest_versions(model_name)
        if not versions:
            return None
        latest = versions[0]
        artifact_uri = client.get_model_version_download_uri(model_name, latest.version)
        path = mlflow.artifacts.download_artifacts(artifact_uri)
        if os.path.isdir(path):
            json_files = [f for f in os.listdir(path) if f.endswith(".json")]
            if not json_files:
                return None
            path = os.path.join(path, json_files[0])
        with open(path) as f:
            data = json.load(f)
        scores = data.get("scores", [])
        log.info("Loaded %d reference scores for %s", len(scores), model_name)
        return scores
    except Exception as e:
        log.warning("Failed to load reference for %s: %s", model_name, e)
        return None


def _query_recent_scores(conn, event_type: str, days: int = 7) -> list[float]:
    cutoff = datetime.now(timezone.utc) - timedelta(days=days)
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT anomaly_score FROM models.inference_log
            WHERE dataset_id = %s AND timestamp > %s AND tier = 'accurate'
            """,
            (event_type, cutoff),
        )
        return [row[0] for row in cur.fetchall()]


def _write_drift_flag(
    conn, model_name: str, model_version: int, event_type: str,
    ks_stat: float, ks_pvalue: float, reference_n: int, recent_n: int,
) -> int:
    now = datetime.now(timezone.utc)
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO models.model_drift_flags
                (model_name, model_version, event_type, ks_stat, ks_pvalue,
                 reference_n, recent_n, detected_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING id
            """,
            (model_name, model_version, event_type, ks_stat, ks_pvalue,
             reference_n, recent_n, now),
        )
        flag_id = cur.fetchone()[0]
    conn.commit()
    return flag_id


@dag(schedule="@weekly", start_date=pendulum_datetime(2025, 1, 1, tz="UTC"), catchup=False, tags=["nexus", "monitoring"])
def aging_monitor_dag():

    @task
    def run_ks_monitor():
        mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)
        client = MlflowClient()
        conn = psycopg2.connect(POSTGRES_META_DSN)
        try:
            for event_type in EVENT_TYPES:
                model_name = f"nexus-anomaly-{event_type}"
                reference_scores = _load_reference_scores(client, model_name)
                if not reference_scores or len(reference_scores) < MIN_SAMPLES:
                    log.info("Skipping %s — no reference (need MLflow checkpoint first)", event_type)
                    continue
                recent_scores = _query_recent_scores(conn, event_type)
                if len(recent_scores) < MIN_SAMPLES:
                    log.info("Skipping %s — only %d recent scores", event_type, len(recent_scores))
                    continue
                ks_stat, ks_pvalue = compute_ks(reference_scores, recent_scores)
                log.info("%s: ks_stat=%.4f pvalue=%.4f ref_n=%d recent_n=%d",
                         event_type, ks_stat, ks_pvalue,
                         len(reference_scores), len(recent_scores))
                if ks_stat > KS_THRESHOLD:
                    versions = client.get_latest_versions(model_name)
                    version = int(versions[0].version) if versions else 0
                    flag_id = _write_drift_flag(
                        conn, model_name, version, event_type,
                        ks_stat, ks_pvalue,
                        len(reference_scores), len(recent_scores),
                    )
                    client.set_model_version_tag(
                        model_name, str(version), "drift_flag", "true"
                    )
                    log.warning(
                        "DRIFT DETECTED: %s ks_stat=%.4f (threshold=%.2f) flag_id=%d",
                        event_type, ks_stat, KS_THRESHOLD, flag_id,
                    )
        finally:
            conn.close()

    run_ks_monitor()


aging_monitor_dag()
