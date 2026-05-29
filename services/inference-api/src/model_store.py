import logging
import os
import pickle
import threading
import time

import mlflow
from mlflow.tracking import MlflowClient

log = logging.getLogger(__name__)

EVENT_TYPES = [
    "OrderPlaced", "PaymentProcessed", "SessionStarted",
    "RecommendationServed", "PriceSnapshot",
]
REFRESH_SECONDS = int(os.environ.get("MODEL_REFRESH_SECONDS", "300"))


class InferenceModelStore:
    """
    Loads River models from MLflow — one production + optional shadow per event type.
    PriceSnapshot always returns None (ADWIN requires stateful update, incompatible with
    read-only inference).
    Background thread refreshes every REFRESH_SECONDS.
    """

    def __init__(self, tracking_uri: str | None = None):
        uri = tracking_uri or os.environ.get("MLFLOW_TRACKING_URI", "http://mlflow:5000")
        mlflow.set_tracking_uri(uri)
        self._client = MlflowClient()
        self._models: dict[str, object | None] = {t: None for t in EVENT_TYPES}
        self._versions: dict[str, int | None] = {t: None for t in EVENT_TYPES}
        self._shadow_models: dict[str, object | None] = {t: None for t in EVENT_TYPES}
        self._shadow_versions: dict[str, int | None] = {t: None for t in EVENT_TYPES}
        self._load_all()
        threading.Thread(target=self._refresh_loop, daemon=True, name="model-refresh").start()

    def _load_all(self) -> None:
        for event_type in EVENT_TYPES:
            if event_type == "PriceSnapshot":
                continue  # ADWIN requires stateful update(); always fallback
            self._load_one(event_type)

    def _download_model(self, model_name: str, version: int) -> object | None:
        try:
            artifact_uri = self._client.get_model_version_download_uri(model_name, version)
            path = mlflow.artifacts.download_artifacts(artifact_uri)
            if os.path.isdir(path):
                files = [f for f in os.listdir(path) if f.endswith(".pkl")]
                if not files:
                    return None
                path = os.path.join(path, files[0])
            with open(path, "rb") as f:
                return pickle.load(f)
        except Exception as e:
            log.warning("Failed to download %s v%s: %s", model_name, version, e)
            return None

    def _load_one(self, event_type: str) -> None:
        name = f"nexus-anomaly-{event_type}"
        try:
            all_versions = self._client.search_model_versions(f"name='{name}'")
            all_versions.sort(key=lambda v: int(v.version), reverse=True)

            prod_loaded = False
            shadow_loaded = False

            for v in all_versions:
                tags = {t.key: t.value for t in v.tags}
                status = tags.get("deployment_status", "production")

                if not prod_loaded and status in ("production", ""):
                    model = self._download_model(name, int(v.version))
                    if model:
                        self._models[event_type] = model
                        self._versions[event_type] = int(v.version)
                        log.info("Loaded production %s v%s", event_type, v.version)
                    prod_loaded = True

                elif not shadow_loaded and status == "shadow":
                    model = self._download_model(name, int(v.version))
                    if model:
                        self._shadow_models[event_type] = model
                        self._shadow_versions[event_type] = int(v.version)
                        log.info("Loaded shadow %s v%s", event_type, v.version)
                    shadow_loaded = True

                if prod_loaded and shadow_loaded:
                    break

        except Exception as e:
            log.warning("Failed to load models for %s: %s", event_type, e)

    def _refresh_loop(self) -> None:
        while True:
            time.sleep(REFRESH_SECONDS)
            self._load_all()

    def get(self, event_type: str) -> tuple[object | None, int | None]:
        """Returns (production_model, version). None → caller uses fallback."""
        return self._models.get(event_type), self._versions.get(event_type)

    def get_shadow(self, event_type: str) -> tuple[object | None, int | None]:
        """Returns (shadow_model, version). None → no shadow deployment active."""
        return self._shadow_models.get(event_type), self._shadow_versions.get(event_type)
