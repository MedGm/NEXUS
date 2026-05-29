import logging
import os
import pickle
import tempfile
import time

import mlflow
from mlflow.tracking import MlflowClient

from .detector import AnomalyDetector, make_model

log = logging.getLogger(__name__)

CHECKPOINT_EVERY = int(os.environ.get("ANOMALY_CHECKPOINT_EVERY", "1000"))
SCORE_BUFFER_SIZE = 250

EVENT_TYPES = [
    "OrderPlaced",
    "PaymentProcessed",
    "SessionStarted",
    "RecommendationServed",
    "PriceSnapshot",
]


class ModelRegistry:
    """
    Manages 5 River AnomalyDetector instances — one per event type.
    Checkpoints each model independently to MLflow every CHECKPOINT_EVERY events.
    On startup, loads the latest checkpoint from MLflow if one exists.
    """

    def __init__(self, tracking_uri: str | None = None):
        uri = tracking_uri or os.environ.get("MLFLOW_TRACKING_URI", "http://mlflow:5000")
        mlflow.set_tracking_uri(uri)
        self._client = MlflowClient()
        self._detectors: dict[str, AnomalyDetector] = {}
        self._counts: dict[str, int] = {t: 0 for t in EVENT_TYPES}
        self._score_buffers: dict[str, list[float]] = {t: [] for t in EVENT_TYPES}
        self._load_or_init_all()

    def _model_name(self, event_type: str) -> str:
        return f"nexus-anomaly-{event_type}"

    def _load_or_init_all(self) -> None:
        for event_type in EVENT_TYPES:
            river_model = self._load_from_mlflow(event_type)
            self._detectors[event_type] = AnomalyDetector(event_type, model=river_model)

    def _load_from_mlflow(self, event_type: str):
        """Returns unpickled River model or None (fresh model) on failure."""
        name = self._model_name(event_type)
        try:
            versions = self._client.get_latest_versions(name)
            if not versions:
                log.info("No checkpoint for %s — starting fresh", event_type)
                return None
            latest = versions[0]
            artifact_uri = self._client.get_model_version_download_uri(name, latest.version)
            path = mlflow.artifacts.download_artifacts(artifact_uri)
            if os.path.isdir(path):
                files = [f for f in os.listdir(path) if f.endswith(".pkl")]
                if not files:
                    return None
                path = os.path.join(path, files[0])
            with open(path, "rb") as f:
                model = pickle.load(f)
            log.info("Loaded %s v%s from MLflow", event_type, latest.version)
            return model
        except Exception as e:
            log.warning("Failed to load %s from MLflow: %s — fresh start", event_type, e)
            return None

    def score_and_learn(self, event_type: str, features: dict[str, float]) -> tuple[float, bool]:
        """Score event, learn from it, checkpoint if count threshold reached."""
        if event_type not in self._detectors:
            raise ValueError(f"Unknown event_type: {event_type!r}")
        score, is_anomaly = self._detectors[event_type].score_one(features)
        # Update score buffer — capped at SCORE_BUFFER_SIZE
        buf = self._score_buffers[event_type]
        buf.append(score)
        if len(buf) > SCORE_BUFFER_SIZE:
            buf.pop(0)
        self._counts[event_type] += 1
        if self._counts[event_type] % CHECKPOINT_EVERY == 0:
            self._checkpoint(event_type)
        return score, is_anomaly

    def _get_score_distribution(self, event_type: str) -> dict:
        """Returns score_distribution dict for storage alongside the pickled model."""
        buf = self._score_buffers[event_type]
        return {
            "scores": list(buf),
            "n_samples": len(buf),
            "event_type": event_type,
            "model_version": str(self._counts[event_type]),
            "captured_at_ms": int(time.time() * 1000),
        }

    def _checkpoint(self, event_type: str) -> None:
        import json as _json
        detector = self._detectors[event_type]
        name = self._model_name(event_type)
        count = self._counts[event_type]
        tmp_path = None
        dist_tmp = None
        try:
            with tempfile.NamedTemporaryFile(suffix=".pkl", delete=False) as f:
                pickle.dump(detector.model, f)
                tmp_path = f.name
            dist = self._get_score_distribution(event_type)
            with tempfile.NamedTemporaryFile(
                mode="w", suffix="_score_dist.json", delete=False
            ) as df:
                _json.dump(dist, df)
                dist_tmp = df.name
            with mlflow.start_run(run_name=f"checkpoint-{event_type}-{count}"):
                mlflow.log_param("event_type", event_type)
                mlflow.log_param("events_seen", count)
                mlflow.log_artifact(tmp_path, artifact_path="model")
                mlflow.log_artifact(dist_tmp, artifact_path="model")
                run_id = mlflow.active_run().info.run_id
                pkl_name = os.path.basename(tmp_path)
                artifact_uri = f"runs:/{run_id}/model/{pkl_name}"
                mlflow.register_model(artifact_uri, name)
            log.info("Checkpointed %s at %d events → %s", event_type, count, name)
        except Exception as e:
            log.error("Checkpoint failed for %s: %s", event_type, e)
        finally:
            for p in (tmp_path, dist_tmp):
                if p:
                    try:
                        os.unlink(p)
                    except Exception:
                        pass
