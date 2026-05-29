import json
import logging
import os
import signal
import struct
import time

from confluent_kafka import Consumer, Producer
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import SerializationContext, MessageField

from nexus_common.kafka import make_schema_registry_client
from .detector import THRESHOLDS
from .features import FeatureExtractor
from .model_store import ModelRegistry

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(message)s")
log = logging.getLogger(__name__)

TOPIC_IN = "nexus.raw.events"
TOPIC_OUT = "nexus.anomalies"
GROUP_ID = "nexus-anomaly-detector"

KNOWN_TYPES = frozenset(THRESHOLDS.keys())

_running = True


def _stop(sig, frame):
    global _running
    _running = False


signal.signal(signal.SIGTERM, _stop)
signal.signal(signal.SIGINT, _stop)


def main() -> None:
    sr = make_schema_registry_client()
    deser = AvroDeserializer(sr)

    consumer = Consumer({
        "bootstrap.servers": os.environ["KAFKA_BOOTSTRAP_SERVERS"],
        "group.id": GROUP_ID,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": True,   # fire-and-forget; anomaly detection is not transactional
    })
    consumer.subscribe([TOPIC_IN])

    producer = Producer({
        "bootstrap.servers": os.environ["KAFKA_BOOTSTRAP_SERVERS"],
        "acks": 1,
    })

    registry = ModelRegistry()
    extractor = FeatureExtractor()
    schema_cache: dict[int, str] = {}

    while _running:
        msg = consumer.poll(timeout=1.0)
        if msg is None:
            continue
        if msg.error():
            log.error("Consumer error: %s", msg.error())
            continue

        raw = msg.value()
        if not raw or len(raw) < 5 or raw[0] != 0x00:
            continue

        # Confluent wire format: [0x00][4-byte schema ID big-endian][avro payload]
        schema_id = struct.unpack(">I", raw[1:5])[0]
        if schema_id not in schema_cache:
            try:
                schema = sr.get_schema(schema_id)
                schema_dict = json.loads(schema.schema_str)
                full_name = schema_dict.get("name", "unknown")
                schema_cache[schema_id] = full_name.split(".")[-1]
            except Exception as e:
                log.warning("Schema lookup failed for id=%d: %s", schema_id, e)
                continue

        event_type = schema_cache[schema_id]
        if event_type not in KNOWN_TYPES:
            continue

        ctx = SerializationContext(TOPIC_IN, MessageField.VALUE)
        record = deser(raw, ctx)
        if record is None:
            continue

        try:
            features = extractor.extract(event_type, record)
            score, is_anomaly = registry.score_and_learn(event_type, features)
        except Exception as e:
            log.error("Detection failed for %s: %s", event_type, e)
            continue

        anomaly_event = json.dumps({
            "event_id": record.get("event_id", ""),
            "event_type": event_type,
            "anomaly_score": score,
            "is_anomaly": is_anomaly,
            "threshold": THRESHOLDS.get(event_type),
            "tier": "accurate",
            "timestamp": record.get("timestamp", int(time.time() * 1000)),
        })
        producer.produce(
            TOPIC_OUT,
            key=(record.get("event_id") or "").encode(),
            value=anomaly_event.encode(),
        )

    producer.flush()
    consumer.close()


if __name__ == "__main__":
    main()
