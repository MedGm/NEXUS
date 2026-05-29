import json
import os
import signal
import logging
import struct
import time
from datetime import datetime, timezone

import psycopg2
from confluent_kafka import Consumer
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import SerializationContext, MessageField

from nexus_common.kafka import make_schema_registry_client
from .validator import GEValidator
from .trust import TrustScoreCalculator, PSI_FLAG_THRESHOLD, MAX_AGE_SECONDS, compute_freshness
from .psi import compute_psi
from .db import write_validation_and_trust, get_reference_window

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(message)s")
log = logging.getLogger(__name__)

TOPIC = "nexus.raw.events"
GROUP_ID = "nexus-validator"
FLUSH_SECONDS = int(os.environ.get("VALIDATOR_FLUSH_SECONDS", "60"))
EXPECTATIONS_DIR = os.path.join(os.path.dirname(__file__), "..", "expectations")

_running = True


def _stop(sig, frame):
    global _running
    _running = False


signal.signal(signal.SIGTERM, _stop)
signal.signal(signal.SIGINT, _stop)


def _flush_type(
    dataset_id: str,
    records: list[dict],
    ge_validator: GEValidator,
    trust_calc: TrustScoreCalculator,
    conn,
) -> None:
    now = datetime.now(timezone.utc)

    # 1. Run GE validation
    val_result = ge_validator.validate(dataset_id, records)
    meta = ge_validator.get_suite_meta(dataset_id)

    # 2. Freshness: age of newest event (timestamp field is epoch ms)
    newest_ts_ms = max(r.get("timestamp", 0) for r in records)
    age_seconds = (now.timestamp() * 1000 - newest_ts_ms) / 1000
    freshness = compute_freshness(age_seconds, MAX_AGE_SECONDS)

    completeness = 1.0 - val_result["null_rate"]

    # 3. PSI — query reference window, compute per psi_column, average
    psi_scores: list[float] = []
    for col in meta["psi_columns"]:
        reference = get_reference_window(conn, dataset_id, col)
        observed = val_result["results_json"]["psi_values"].get(col, [])
        psi = compute_psi(
            observed,
            reference,
            min_reference_samples=meta["min_reference_batches"] * 10,
        )
        if psi is not None:
            if psi > PSI_FLAG_THRESHOLD:
                log.warning(
                    "PSI flag: dataset=%s col=%s psi=%.4f > threshold=%.2f",
                    dataset_id, col, psi, PSI_FLAG_THRESHOLD,
                )
            psi_scores.append(psi)

    drift_psi_raw = (sum(psi_scores) / len(psi_scores)) if psi_scores else None

    # 4. Compute composite
    scores = trust_calc.compute(
        freshness=freshness,
        completeness=completeness,
        drift_psi=drift_psi_raw,
        lineage_depth=meta["lineage_depth"],
    )

    # 5. Write to Postgres (single transaction) — offset commit happens AFTER this
    write_validation_and_trust(
        conn=conn,
        dataset_id=dataset_id,
        batch_timestamp=now,
        batch_size=val_result["batch_size"],
        success=val_result["success"],
        null_rate=val_result["null_rate"],
        results_json=val_result["results_json"],
        freshness=scores["freshness"],
        completeness=scores["completeness"],
        drift_psi_score=scores["drift_psi"],
        lineage_depth=scores["lineage_depth"],
        composite_score=scores["composite_score"],
    )

    log.info(
        "Flushed dataset=%s size=%d success=%s composite=%.3f",
        dataset_id,
        val_result["batch_size"],
        val_result["success"],
        scores["composite_score"],
    )


def _extract_schema_id(raw_bytes: bytes) -> int:
    """Extract Confluent Schema Registry schema ID from Avro-encoded message bytes.

    Wire format: [magic_byte=0x00][schema_id: 4 bytes big-endian][avro_payload...]
    """
    return struct.unpack(">I", raw_bytes[1:5])[0]


def main() -> None:
    sr = make_schema_registry_client()
    deser = AvroDeserializer(sr)

    consumer = Consumer({
        "bootstrap.servers": os.environ["KAFKA_BOOTSTRAP_SERVERS"],
        "group.id": GROUP_ID,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    })
    consumer.subscribe([TOPIC])

    conn = psycopg2.connect(os.environ["POSTGRES_DSN"])

    ge_validator = GEValidator(EXPECTATIONS_DIR)
    trust_calc = TrustScoreCalculator()

    # Cache schema_id -> short record name to avoid redundant registry lookups
    _schema_name_cache: dict[int, str] = {}

    buffers: dict[str, list[dict]] = {}
    flush_at = time.monotonic() + FLUSH_SECONDS

    while _running:
        msg = consumer.poll(timeout=1.0)
        if msg is not None and not msg.error():
            raw = msg.value()
            if raw and len(raw) >= 5 and raw[0] == 0x00:
                schema_id = _extract_schema_id(raw)
                if schema_id not in _schema_name_cache:
                    schema = sr.get_schema(schema_id)
                    schema_dict = json.loads(schema.schema_str)
                    full_name = schema_dict.get("name", "unknown")
                    _schema_name_cache[schema_id] = full_name.split(".")[-1]
                short_name = _schema_name_cache[schema_id]
                ctx = SerializationContext(TOPIC, MessageField.VALUE)
                record = deser(raw, ctx)
                if record:
                    buffers.setdefault(short_name, []).append(record)
        elif msg is not None and msg.error():
            log.error("Consumer error: %s", msg.error())

        if time.monotonic() >= flush_at:
            flush_at = time.monotonic() + FLUSH_SECONDS
            for event_type, records in list(buffers.items()):
                if records:
                    try:
                        _flush_type(event_type, records, ge_validator, trust_calc, conn)
                    except Exception as e:
                        log.error("Flush failed for %s: %s", event_type, e)
                        conn.rollback()
                        continue
                    buffers[event_type] = []
            # Kafka offset commit AFTER all Postgres writes succeed
            consumer.commit(asynchronous=False)

    # Final flush on shutdown
    for event_type, records in buffers.items():
        if records:
            try:
                _flush_type(event_type, records, ge_validator, trust_calc, conn)
            except Exception as e:
                log.error("Final flush failed for %s: %s", event_type, e)
    consumer.close()
    conn.close()


if __name__ == "__main__":
    main()
