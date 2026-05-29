import logging
import os
import threading
import time

from fastapi import FastAPI, HTTPException
from fastapi.responses import Response
from prometheus_client import Gauge, generate_latest, CONTENT_TYPE_LATEST

from .db import get_conn, get_latest_trust_score, get_all_latest_trust_scores, get_psi_flag_count

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(message)s")
log = logging.getLogger(__name__)

app = FastAPI(title="NEXUS Trust API")

PSI_FLAG_THRESHOLD = 0.20

_trust_composite = Gauge("nexus_trust_composite", "Composite trust score", ["dataset"])
_trust_freshness = Gauge("nexus_trust_freshness", "Freshness sub-score", ["dataset"])
_trust_completeness = Gauge("nexus_trust_completeness", "Completeness sub-score", ["dataset"])
_psi_flags_total = Gauge("nexus_psi_flags_total", "Total PSI flag count", ["dataset"])


def _update_prometheus_metrics() -> None:
    while True:
        try:
            conn = get_conn()
            scores = get_all_latest_trust_scores(conn)
            for s in scores:
                d = s["dataset_id"]
                _trust_composite.labels(dataset=d).set(s["composite_score"])
                _trust_freshness.labels(dataset=d).set(s["freshness"])
                _trust_completeness.labels(dataset=d).set(s["completeness"])
                flag_count = get_psi_flag_count(conn, d, PSI_FLAG_THRESHOLD)
                _psi_flags_total.labels(dataset=d).set(flag_count)
            conn.close()
        except Exception as e:
            log.warning("Prometheus metrics update failed: %s", e)
        time.sleep(30)


threading.Thread(target=_update_prometheus_metrics, daemon=True, name="prom-updater").start()


@app.get("/trust/{dataset_id}/latest")
def get_trust_latest(dataset_id: str):
    try:
        conn = get_conn()
        score = get_latest_trust_score(conn, dataset_id)
        conn.close()
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"Database error: {e}")
    if score is None:
        raise HTTPException(status_code=404, detail=f"No trust scores found for {dataset_id!r}")
    # Convert timestamp to ISO string for JSON serialization
    if score.get("timestamp"):
        score["timestamp"] = score["timestamp"].isoformat()
    return score


@app.get("/metrics")
def prometheus_metrics():
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)


@app.get("/health")
def health():
    return {"status": "ok"}
