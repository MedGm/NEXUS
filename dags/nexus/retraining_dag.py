import json
import logging
import os
import pickle
import tempfile
import time
from datetime import datetime, timedelta, timezone

from pendulum import datetime as pendulum_datetime

import boto3
import mlflow
import psycopg2
from airflow.decorators import dag, task
from airflow.exceptions import AirflowException
from airflow.models.param import Param
from mlflow.tracking import MlflowClient
from river.anomaly import HalfSpaceTrees

try:
    from .dag_helpers import detect_event_type, extract_features_for_type, HST_PARAMS, SCORE_BUFFER_SIZE
except ImportError:
    import sys as _sys, os as _os
    _sys.path.insert(0, _os.path.join(_os.path.dirname(__file__), ".."))
    from nexus.dag_helpers import detect_event_type, extract_features_for_type, HST_PARAMS, SCORE_BUFFER_SIZE

log = logging.getLogger(__name__)

POSTGRES_META_DSN = os.environ.get("POSTGRES_META_DSN", "postgresql://postgres:postgres@postgres/nexus_meta")
MLFLOW_TRACKING_URI = os.environ.get("MLFLOW_TRACKING_URI", "http://mlflow:5000")
MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY = os.environ.get("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.environ.get("MINIO_SECRET_KEY", "minioadmin")
RAW_DATA_BUCKET = "raw-data"
SHADOW_COMPLETION_N = int(os.environ.get("SHADOW_COMPLETION_N", "1000"))
EXPECTATIONS_DIR = os.path.join(os.path.dirname(__file__), "expectations")


@dag(
    schedule=None,
    start_date=pendulum_datetime(2025, 1, 1, tz="UTC"),
    catchup=False,
    tags=["nexus", "retraining"],
    params={"event_type": Param(
        default="OrderPlaced",
        type="string",
        description="Event type to retrain: OrderPlaced, PaymentProcessed, SessionStarted, RecommendationServed, PriceSnapshot",
    )},
)
def retraining_dag():

    @task
    def get_flag_id(event_type: str) -> int:
        conn = psycopg2.connect(POSTGRES_META_DSN)
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT id FROM models.model_drift_flags
                    WHERE event_type = %s AND status = 'flagged'
                    ORDER BY detected_at DESC LIMIT 1
                    """,
                    (event_type,),
                )
                row = cur.fetchone()
            if row is None:
                raise AirflowException(
                    f"No unresolved drift flag for {event_type!r}. "
                    "Run aging_monitor_dag first, or insert a test flag."
                )
            log.info("Using drift_flag_id=%d for %s", row[0], event_type)
            return row[0]
        finally:
            conn.close()

    @task
    def fetch_data(event_type: str) -> str:
        s3 = boto3.client(
            "s3",
            endpoint_url=MINIO_ENDPOINT,
            aws_access_key_id=MINIO_ACCESS_KEY,
            aws_secret_access_key=MINIO_SECRET_KEY,
        )
        cutoff = (datetime.now(timezone.utc) - timedelta(days=7)).timestamp() * 1000
        paginator = s3.get_paginator("list_objects_v2")
        records = []
        for page in paginator.paginate(Bucket=RAW_DATA_BUCKET):
            for obj in page.get("Contents", []):
                try:
                    response = s3.get_object(Bucket=RAW_DATA_BUCKET, Key=obj["Key"])
                    for raw_line in response["Body"].iter_lines():
                        try:
                            record = json.loads(raw_line)
                            if detect_event_type(record) == event_type:
                                if record.get("timestamp", 0) >= cutoff:
                                    records.append(record)
                        except Exception:
                            continue
                except Exception as e:
                    log.warning("Failed to read %s: %s", obj["Key"], e)
                    continue
        if len(records) < 10:
            raise AirflowException(
                f"Insufficient data for {event_type}: only {len(records)} records in last 7 days"
            )
        tmp = tempfile.mktemp(suffix=f"_{event_type}.jsonl")
        with open(tmp, "w") as f:
            for r in records:
                f.write(json.dumps(r) + "\n")
        log.info("Fetched %d records for %s → %s", len(records), event_type, tmp)
        return tmp

    @task
    def validate_data(data_path: str, event_type: str) -> str:
        import yaml
        import pandas as pd
        import great_expectations as gx

        yml_path = os.path.join(EXPECTATIONS_DIR, f"{event_type}.yml")
        with open(yml_path) as f:
            suite = yaml.safe_load(f)
        records = []
        with open(data_path) as f:
            for line in f:
                try:
                    records.append(json.loads(line.strip()))
                except Exception:
                    continue
        df = pd.DataFrame(records)
        ge_df = gx.from_pandas(df)
        failed = []
        for exp in suite.get("expectations", []):
            try:
                method = getattr(ge_df, exp["type"])
                result = method(**exp.get("kwargs", {}))
                if not result.success:
                    failed.append(exp["type"])
            except Exception as e:
                log.warning("Expectation error %s: %s", exp["type"], e)
                failed.append(exp["type"])
        if failed:
            raise AirflowException(f"GE validation failed for {event_type}: {failed}")
        log.info("Validation passed for %s (%d records)", event_type, len(records))
        return data_path

    @task
    def retrain_model(data_path: str, event_type: str) -> str:
        mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)
        records = []
        with open(data_path) as f:
            for line in f:
                try:
                    records.append(json.loads(line.strip()))
                except Exception:
                    continue
        features_list = extract_features_for_type(event_type, records)
        if not features_list:
            raise AirflowException(f"No features extracted for {event_type}")
        model = HalfSpaceTrees(**HST_PARAMS)
        score_buffer: list[float] = []
        for features in features_list:
            score = float(model.score_one(features))
            model.learn_one(features)
            score_buffer.append(score)
            if len(score_buffer) > SCORE_BUFFER_SIZE:
                score_buffer.pop(0)
        tmp_pkl = None
        tmp_json = None
        try:
            with tempfile.NamedTemporaryFile(suffix=".pkl", delete=False) as f:
                pickle.dump(model, f)
                tmp_pkl = f.name
            dist = {
                "scores": score_buffer,
                "n_samples": len(score_buffer),
                "event_type": event_type,
                "model_version": "retrain",
                "captured_at_ms": int(time.time() * 1000),
            }
            with tempfile.NamedTemporaryFile(mode="w", suffix="_score_dist.json", delete=False) as f:
                json.dump(dist, f)
                tmp_json = f.name
            with mlflow.start_run(run_name=f"retrain-{event_type}"):
                mlflow.log_param("event_type", event_type)
                mlflow.log_param("training_records", len(records))
                mlflow.log_artifact(tmp_pkl, artifact_path="model")
                mlflow.log_artifact(tmp_json, artifact_path="model")
                run_id = mlflow.active_run().info.run_id
        finally:
            for p in (tmp_pkl, tmp_json, data_path):
                if p:
                    try:
                        os.unlink(p)
                    except Exception:
                        pass
        log.info("Retrained %s run_id=%s (%d records)", event_type, run_id, len(records))
        return run_id

    @task
    def register_shadow(mlflow_run_id: str, event_type: str, drift_flag_id: int) -> int:
        mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)
        client = MlflowClient()
        model_name = f"nexus-anomaly-{event_type}"
        artifacts = client.list_artifacts(mlflow_run_id, "model")
        pkl_artifact = next((a for a in artifacts if a.path.endswith(".pkl")), None)
        if not pkl_artifact:
            raise AirflowException(f"No pkl artifact in run {mlflow_run_id}")
        artifact_uri = f"runs:/{mlflow_run_id}/{pkl_artifact.path}"
        result = mlflow.register_model(artifact_uri, model_name)
        new_version = str(result.version)
        client.set_model_version_tag(model_name, new_version, "deployment_status", "shadow")
        conn = psycopg2.connect(POSTGRES_META_DSN)
        try:
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE models.model_drift_flags SET status='shadow' WHERE id=%s",
                    (drift_flag_id,),
                )
            conn.commit()
        finally:
            conn.close()
        log.info("Registered %s v%s as shadow (drift_flag_id=%d)", model_name, new_version, drift_flag_id)
        return int(new_version)

    @task
    def await_shadow_completion(event_type: str, shadow_version: int, drift_flag_id: int):
        log.info(
            "Waiting for %d shadow requests for %s v%s (drift_flag_id=%d)",
            SHADOW_COMPLETION_N, event_type, shadow_version, drift_flag_id,
        )
        conn = psycopg2.connect(POSTGRES_META_DSN)
        try:
            timeout_s = 7 * 24 * 3600
            elapsed = 0
            while elapsed < timeout_s:
                with conn.cursor() as cur:
                    cur.execute(
                        "SELECT COUNT(*) FROM models.shadow_log WHERE drift_flag_id = %s",
                        (drift_flag_id,),
                    )
                    count = cur.fetchone()[0]
                if count >= SHADOW_COMPLETION_N:
                    break
                log.info("Shadow requests: %d / %d", count, SHADOW_COMPLETION_N)
                time.sleep(300)
                elapsed += 300
            else:
                log.warning("Shadow evaluation timeout after 7 days")
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT AVG(production_score), AVG(shadow_score),
                           AVG(shadow_latency_ms), COUNT(*)
                    FROM models.shadow_log WHERE drift_flag_id = %s
                    """,
                    (drift_flag_id,),
                )
                avg_prod, avg_shadow, avg_latency, n = cur.fetchone()
            log.info(
                "Shadow summary: n=%d avg_prod=%.4f avg_shadow=%.4f avg_latency=%.1fms",
                n or 0, avg_prod or 0, avg_shadow or 0, avg_latency or 0,
            )
            log.info("Review above and trigger promote_model_dag to promote or dismiss.")
        finally:
            conn.close()

    et = "{{ params.event_type }}"
    flag_id = get_flag_id(et)
    data = fetch_data(et)
    validated = validate_data(data, et)
    run_id_xcom = retrain_model(validated, et)
    shadow_v = register_shadow(run_id_xcom, et, flag_id)
    await_shadow_completion(et, shadow_v, flag_id)


retraining_dag()
