import logging
import os
from datetime import datetime, timezone

from confluent_kafka import Producer
from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import Response
from prometheus_client import Counter, Gauge, generate_latest, CONTENT_TYPE_LATEST
from pydantic import BaseModel

from .consumer import start_background_threads, _add_node_to_graph
from .db import (
    get_incidents_conn,
    get_incident, list_incidents, resolve_incident, update_llm_summary,
)
from .narrator import generate_summary
from .replay import replay_incident
from .taxonomy import node_from_event_payload

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(message)s")
log = logging.getLogger(__name__)

app = FastAPI(title="NEXUS Incident Engine")

_producer = Producer({
    "bootstrap.servers": os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092"),
    "acks": 1,
})

_g_open = Gauge("nexus_open_incidents", "Currently open incidents")
_c_closed = Counter("nexus_closed_incidents_total", "Closed incidents", ["severity"])


@app.on_event("startup")
def startup():
    start_background_threads(_producer)
    log.info("incident-engine started on :8002")


@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/metrics")
def metrics():
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)


@app.get("/incidents")
def get_incidents_list(
    status: str | None = Query(None, description="Filter: open or resolved"),
    limit: int = Query(20, ge=1, le=100),
):
    try:
        conn = get_incidents_conn()
        result = list_incidents(conn, status=status, limit=limit)
        conn.close()
        return result
    except Exception as e:
        raise HTTPException(status_code=503, detail=str(e))


@app.get("/incidents/{incident_id}")
def get_incident_detail(incident_id: str):
    try:
        conn = get_incidents_conn()
        row = get_incident(conn, incident_id)
        conn.close()
    except Exception as e:
        raise HTTPException(status_code=503, detail=str(e))
    if row is None:
        raise HTTPException(status_code=404, detail="Incident not found")
    return row


@app.get("/incidents/{incident_id}/graph")
def get_incident_graph(incident_id: str):
    try:
        conn = get_incidents_conn()
        row = get_incident(conn, incident_id)
        conn.close()
    except Exception as e:
        raise HTTPException(status_code=503, detail=str(e))
    if row is None:
        raise HTTPException(status_code=404, detail="Incident not found")
    return row["causal_graph_json"]


@app.get("/incidents/{incident_id}/summary")
def get_incident_summary(incident_id: str):
    try:
        conn = get_incidents_conn()
        row = get_incident(conn, incident_id)
    except Exception as e:
        raise HTTPException(status_code=503, detail=str(e))
    if row is None:
        conn.close()
        raise HTTPException(status_code=404, detail="Incident not found")
    if row["llm_summary"] is None:
        summary = generate_summary(
            row["causal_graph_json"],
            {"root_cause_node_id": row["root_cause_node_id"],
             "severity": row["severity"], "duration_seconds": 0},
        )
        if summary:
            update_llm_summary(conn, incident_id, summary)
        conn.close()
        return {"llm_summary": summary}
    conn.close()
    return {"llm_summary": row["llm_summary"]}


@app.post("/incidents/{incident_id}/replay")
def post_replay(incident_id: str):
    try:
        conn = get_incidents_conn()
        row = get_incident(conn, incident_id)
        conn.close()
    except Exception as e:
        raise HTTPException(status_code=503, detail=str(e))
    if row is None:
        raise HTTPException(status_code=404, detail="Incident not found")
    try:
        return replay_incident(row)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Replay failed: {e}")


@app.post("/incidents/{incident_id}/resolve")
def post_resolve(incident_id: str):
    try:
        conn = get_incidents_conn()
        updated = resolve_incident(conn, incident_id)
        conn.close()
    except Exception as e:
        raise HTTPException(status_code=503, detail=str(e))
    if not updated:
        raise HTTPException(status_code=404, detail="Incident not found or already resolved")
    return {"status": "resolved", "resolved_at": datetime.now(timezone.utc).isoformat()}


class EventPayload(BaseModel):
    type: str
    dataset_id: str = "all"
    metadata: dict = {}
    severity: str = "MEDIUM"


@app.post("/events")
def post_event(payload: EventPayload):
    try:
        node = node_from_event_payload(payload.model_dump())
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    _add_node_to_graph(node, _producer)
    return {"node_id": node["node_id"], "type": node["type"], "injected": True}
