import logging
import os
import threading
import time
from datetime import datetime, timezone

from cachetools import TTLCache
from fastapi import FastAPI, HTTPException
from fastapi.responses import Response
from prometheus_client import Gauge, generate_latest, CONTENT_TYPE_LATEST
from pydantic import BaseModel

from .db import (get_metrics_conn, get_meta_conn, get_trust_score,
                 write_inference_log, get_active_shadow_flag_id, write_shadow_log)
from .fallback import StatisticalFallback
from .model_store import InferenceModelStore
from .router import TwoTierRouter

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(message)s")
log = logging.getLogger(__name__)

app = FastAPI(title="NEXUS Inference API")

# Thresholds must match anomaly-detector env vars
_THRESHOLDS = {
    "OrderPlaced":          float(os.environ.get("THRESHOLD_ORDER", "0.75")),
    "PaymentProcessed":     float(os.environ.get("THRESHOLD_PAYMENT", "0.80")),
    "SessionStarted":       float(os.environ.get("THRESHOLD_SESSION", "0.75")),
    "RecommendationServed": float(os.environ.get("THRESHOLD_REC", "0.70")),
}

_model_store = InferenceModelStore()
_fallback = StatisticalFallback()
_router = TwoTierRouter()
_trust_cache: TTLCache = TTLCache(maxsize=100, ttl=30)

_g_inferences = Gauge("nexus_inference_total", "Total predictions", ["dataset", "tier"])
_g_anomalies = Gauge("nexus_inference_anomalies_total", "Total anomalies", ["dataset", "tier"])
_g_rejections = Gauge("nexus_inference_trust_gate_rejections_total", "Trust gate rejections", ["dataset"])
_g_score = Gauge("nexus_anomaly_score", "Rolling mean anomaly score (last 100)", ["dataset"])

_counts: dict = {}
_score_windows: dict[str, list[float]] = {}
_shadow_flag_ids: dict[str, int | None] = {}   # dataset_id → drift_flag_id (cached)


def _log_shadow(
    event_id: str,
    dataset_id: str,
    production_score: float,
    shadow_score: float,
    shadow_version: int,
    shadow_latency_ms: int,
) -> None:
    """Write shadow result to shadow_log. Best-effort — never raises."""
    try:
        flag_id = _shadow_flag_ids.get(dataset_id, -1)
        if flag_id == -1:   # not yet cached
            conn = get_meta_conn()
            flag_id = get_active_shadow_flag_id(conn, dataset_id)
            conn.close()
            _shadow_flag_ids[dataset_id] = flag_id
        if flag_id is None:
            return  # no active shadow for this dataset
        conn = get_meta_conn()
        write_shadow_log(
            conn=conn,
            drift_flag_id=flag_id,
            event_id=event_id,
            dataset_id=dataset_id,
            production_score=production_score,
            shadow_score=shadow_score,
            shadow_model_version=shadow_version,
            shadow_latency_ms=shadow_latency_ms,
            timestamp=datetime.now(timezone.utc),
        )
        conn.close()
    except Exception as e:
        log.warning("Shadow log write failed for %s: %s", dataset_id, e)


class PredictRequest(BaseModel):
    dataset_id: str
    event_id: str
    features: dict[str, float]


def _get_trust(dataset_id: str) -> float | None:
    if dataset_id in _trust_cache:
        return _trust_cache[dataset_id]
    try:
        conn = get_metrics_conn()
        score = get_trust_score(conn, dataset_id)
        conn.close()
    except Exception as e:
        log.warning("Trust score fetch failed for %s: %s", dataset_id, e)
        score = None
    _trust_cache[dataset_id] = score
    return score


@app.post("/predict")
def predict(req: PredictRequest):
    dataset_id = req.dataset_id

    trust_score = _get_trust(dataset_id)
    tier, rejection_reason = _router.route(dataset_id, req.features, trust_score)

    if tier == "reject":
        k = ("rej", dataset_id)
        _counts[k] = _counts.get(k, 0) + 1
        _g_rejections.labels(dataset=dataset_id).set(_counts[k])
        raise HTTPException(status_code=422, detail=rejection_reason)

    model, model_version = _model_store.get(dataset_id)

    if tier == "accurate" and model is not None:
        # HalfSpaceTrees.score_one() is read-only (does not mutate model state)
        raw_score = float(model.score_one(req.features))
        threshold = _THRESHOLDS.get(dataset_id, 0.75)
        is_anomaly = raw_score >= threshold
        used_tier = "accurate"
        used_model_name = f"nexus-anomaly-{dataset_id}"
        used_version = model_version
    else:
        # Fallback: no model loaded, PriceSnapshot (ADWIN read-only unsupported), or high lag
        raw_score = _fallback.score(dataset_id, req.features)
        is_anomaly = raw_score >= 0.5
        used_tier = "fallback"
        used_model_name = "fallback"
        used_version = None

    # Update in-memory Prometheus counters
    ck = (dataset_id, used_tier)
    _counts[ck] = _counts.get(ck, 0) + 1
    if is_anomaly:
        _counts[("anom", *ck)] = _counts.get(("anom", *ck), 0) + 1
    _g_inferences.labels(dataset=dataset_id, tier=used_tier).set(_counts[ck])
    _g_anomalies.labels(dataset=dataset_id, tier=used_tier).set(_counts.get(("anom", *ck), 0))

    win = _score_windows.setdefault(dataset_id, [])
    win.append(raw_score)
    if len(win) > 100:
        win.pop(0)
    _g_score.labels(dataset=dataset_id).set(sum(win) / len(win))

    # Log inference to Postgres (best-effort — do not fail the request on DB error)
    try:
        conn = get_meta_conn()
        write_inference_log(
            conn=conn,
            dataset_id=dataset_id,
            event_id=req.event_id,
            model_name=used_model_name,
            model_version=used_version,
            anomaly_score=raw_score,
            is_anomaly=is_anomaly,
            tier=used_tier,
            trust_score_at_inference=float(trust_score) if trust_score is not None else 0.0,
            consumer_lag_ms=int(req.features.get("consumer_lag_ms", 0)) or None,
            timestamp=datetime.now(timezone.utc),
        )
        conn.close()
    except Exception as e:
        log.warning("Inference log write failed: %s", e)

    # Shadow scoring — best-effort, result not returned to caller
    shadow_model, shadow_version_s = _model_store.get_shadow(dataset_id)
    if shadow_model is not None and shadow_version_s is not None:
        try:
            t0 = time.monotonic()
            shadow_score = float(shadow_model.score_one(req.features))
            shadow_latency_ms = int((time.monotonic() - t0) * 1000)
            _log_shadow(req.event_id, dataset_id, raw_score,
                        shadow_score, shadow_version_s, shadow_latency_ms)
        except Exception as e:
            log.warning("Shadow scoring failed for %s: %s", dataset_id, e)

    return {
        "anomaly_score": raw_score,
        "is_anomaly": is_anomaly,
        "tier": used_tier,
        "trust_score_at_inference": trust_score,
        "model_version": used_version,
        "model_name": used_model_name,
    }


@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/metrics")
def prom_metrics():
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)
