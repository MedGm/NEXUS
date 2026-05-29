import logging
import os
from datetime import datetime, timezone

from pendulum import datetime as pendulum_datetime

import mlflow
import psycopg2
from airflow.decorators import dag, task
from airflow.exceptions import AirflowException
from airflow.models.param import Param
from mlflow.tracking import MlflowClient

log = logging.getLogger(__name__)

POSTGRES_META_DSN = os.environ.get("POSTGRES_META_DSN", "postgresql://postgres:postgres@postgres/nexus_meta")
MLFLOW_TRACKING_URI = os.environ.get("MLFLOW_TRACKING_URI", "http://mlflow:5000")


@dag(
    schedule=None,
    start_date=pendulum_datetime(2025, 1, 1, tz="UTC"),
    catchup=False,
    tags=["nexus", "promotion"],
    params={"event_type": Param(
        default="OrderPlaced",
        type="string",
        description="Event type to promote from shadow to production",
    )},
)
def promote_model_dag():

    @task
    def promote(event_type: str):
        mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)
        client = MlflowClient()
        model_name = f"nexus-anomaly-{event_type}"
        all_versions = client.search_model_versions(f"name='{model_name}'")
        shadow_version = None
        for v in all_versions:
            tags = {t.key: t.value for t in v.tags}
            if tags.get("deployment_status") == "shadow":
                shadow_version = str(v.version)
                break
        if shadow_version is None:
            raise AirflowException(
                f"No shadow version found for {model_name}. Run retraining_dag first."
            )
        client.set_model_version_tag(model_name, shadow_version, "deployment_status", "production")
        conn = psycopg2.connect(POSTGRES_META_DSN)
        try:
            now = datetime.now(timezone.utc)
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE models.model_drift_flags
                    SET status = 'promoted', resolved_at = %s
                    WHERE event_type = %s AND status = 'shadow'
                    """,
                    (now, event_type),
                )
            conn.commit()
        finally:
            conn.close()
        log.info(
            "Promoted %s v%s to production. inference-api picks up on next 5min refresh.",
            model_name, shadow_version,
        )

    promote("{{ params.event_type }}")


promote_model_dag()
