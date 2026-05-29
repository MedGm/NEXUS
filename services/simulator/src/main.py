import os
import signal
import threading
import time
import logging

import uvicorn
from fastapi import FastAPI
from pydantic import BaseModel

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(message)s")
log = logging.getLogger(__name__)

TOPIC = "nexus.raw.events"
SCHEMA_DIR = os.path.join(os.path.dirname(__file__), "schemas")

# Mutable config — read by producer loop on every iteration
_config: dict = {
    "base_rate": float(os.environ.get("SIMULATOR_BASE_RATE", "10")),
    "burst_probability": 0.05,
    "burst_size": 500.0,
}
_config_lock = threading.Lock()

_running = True


def _stop(sig, frame):
    global _running
    _running = False
    log.info("Shutdown signal received")


signal.signal(signal.SIGTERM, _stop)
signal.signal(signal.SIGINT, _stop)

# ── Control API ──────────────────────────────────────────────────────────────

control_app = FastAPI(title="Simulator Control")


class ControlBody(BaseModel):
    base_rate: float | None = None
    burst_probability: float | None = None
    burst_size: float | None = None


@control_app.get("/control")
def get_config():
    with _config_lock:
        return dict(_config)


@control_app.post("/control")
def update_config(body: ControlBody):
    with _config_lock:
        if body.base_rate is not None:
            _config["base_rate"] = body.base_rate
        if body.burst_probability is not None:
            _config["burst_probability"] = body.burst_probability
        if body.burst_size is not None:
            _config["burst_size"] = body.burst_size
        return dict(_config)


# ── Producer loop ─────────────────────────────────────────────────────────────

def main() -> None:
    # Kafka/Avro imports deferred so that importing this module for tests
    # does not require confluent_kafka's heavy optional dependencies.
    from nexus_common.kafka import make_schema_registry_client, make_producer
    from nexus_common.avro import get_serializer, serialize
    from .simulator import EventSimulator
    from .temporal import rate_multiplier

    sr = make_schema_registry_client()
    producer = make_producer()

    serializers = {
        name: get_serializer(f"{SCHEMA_DIR}/{name}.avsc", sr)
        for name in ("OrderPlaced", "PaymentProcessed", "SessionStarted", "RecommendationServed")
    }

    sim = EventSimulator()
    count = 0

    while _running:
        event = sim.next()
        event_type = type(event).__name__
        value_bytes = serialize(serializers[event_type], TOPIC, event.to_dict())

        producer.produce(
            topic=TOPIC,
            key=event.event_id.encode(),
            value=value_bytes,
            on_delivery=lambda err, _: log.error("Delivery error: %s", err) if err else None,
        )
        producer.poll(0)
        count += 1

        if count % 500 == 0:
            log.info("Produced %d events", count)

        with _config_lock:
            base_rate = _config["base_rate"]
        sleep_s = 1.0 / (base_rate * rate_multiplier())
        time.sleep(sleep_s)

    producer.flush()
    log.info("Flushed. Total produced: %d", count)


if __name__ == "__main__":
    producer_thread = threading.Thread(target=main, daemon=True, name="producer")
    producer_thread.start()
    port = int(os.environ.get("SIMULATOR_CONTROL_PORT", "8003"))
    log.info("Control API on :%d", port)
    uvicorn.run(control_app, host="0.0.0.0", port=port, log_level="warning")
