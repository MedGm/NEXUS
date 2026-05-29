import os
import signal
import time
import logging
import httpx

from nexus_common.kafka import make_schema_registry_client, make_producer
from nexus_common.avro import get_serializer, serialize
from .poller import fetch_prices

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(message)s")
log = logging.getLogger(__name__)

TOPIC = "nexus.raw.events"
INTERVAL = int(os.environ.get("POLLER_INTERVAL_SECONDS", "60"))
SCHEMA_DIR = os.path.join(os.path.dirname(__file__), "schemas")

_running = True


def _stop(sig, frame):
    global _running
    _running = False


signal.signal(signal.SIGTERM, _stop)
signal.signal(signal.SIGINT, _stop)


def main() -> None:
    sr = make_schema_registry_client()
    producer = make_producer()
    serializer = get_serializer(f"{SCHEMA_DIR}/PriceSnapshot.avsc", sr)

    with httpx.Client() as client:
        while _running:
            try:
                events = fetch_prices(client)
                for event in events:
                    value_bytes = serialize(serializer, TOPIC, event)
                    producer.produce(
                        topic=TOPIC,
                        key=event["event_id"].encode(),
                        value=value_bytes,
                        on_delivery=lambda err, _: log.error("Delivery error: %s", err) if err else None,
                    )
                producer.flush()
                log.info("Published %d PriceSnapshot events", len(events))
            except httpx.HTTPError as exc:
                log.warning("CoinGecko request failed: %s — skipping cycle", exc)

            time.sleep(INTERVAL)


if __name__ == "__main__":
    main()
