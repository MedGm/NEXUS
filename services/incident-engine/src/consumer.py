import json
import logging
import os
import threading
import time
from datetime import datetime, timezone

from confluent_kafka import Consumer, Producer

from .db import (
    get_incidents_conn, get_meta_conn, get_metrics_conn,
    persist_incident, update_llm_summary,
    get_last_trust_scores, get_new_drift_flags,
)
from .graph import CausalGraphBuilder
from .narrator import generate_summary
from .taxonomy import (
    anomaly_event_to_node,
    make_trust_drop_node,
    make_kafka_lag_node,
    make_prediction_drift_node,
)

log = logging.getLogger(__name__)

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
TOPIC_IN = "nexus.anomalies"
TOPIC_OUT = "nexus.incidents"
GROUP_ID = "nexus-incident-engine"
INCIDENT_IDLE_TIMEOUT = int(os.environ.get("INCIDENT_IDLE_TIMEOUT_SECONDS", "600"))
KAFKA_LAG_THRESHOLD = int(os.environ.get("KAFKA_LAG_THRESHOLD", "5000"))
TRUST_DROP_THRESHOLD = float(os.environ.get("TRUST_DROP_THRESHOLD", "0.15"))
POLL_INTERVAL = 60

DATASET_IDS = [
    "OrderPlaced", "PaymentProcessed", "SessionStarted",
    "RecommendationServed", "PriceSnapshot",
]

_running = True
_lock = threading.Lock()
_builder = CausalGraphBuilder()      # single global graph / pending incident
_last_drift_flag_id: int = 0


def _add_node_to_graph(node: dict, producer: Producer) -> None:
    global _builder
    with _lock:
        _builder.add_node(node)


def _close_incident(builder: CausalGraphBuilder, producer: Producer) -> None:
    graph_json = builder.to_json()
    root_node_id = builder.root_cause_node_id()
    severity = builder.max_severity()
    node_count = builder.node_count()
    now = datetime.now(timezone.utc)
    started_at = builder.started_at or now
    duration_s = int((now - started_at).total_seconds())

    incident_meta = {
        "root_cause_node_id": root_node_id,
        "severity": severity,
        "duration_seconds": duration_s,
    }
    llm_summary = generate_summary(graph_json, incident_meta)

    try:
        conn = get_incidents_conn()
        persist_incident(
            conn=conn,
            incident_id=builder.incident_id,
            started_at=started_at,
            resolved_at=None,
            root_cause_node_id=root_node_id,
            causal_graph_json=graph_json,
            llm_summary=llm_summary,
            severity=severity,
            node_count=node_count,
        )
        conn.close()
        log.info("Incident %s persisted (nodes=%d severity=%s)", builder.incident_id, node_count, severity)
    except Exception as e:
        log.error("Failed to persist incident %s: %s", builder.incident_id, e)

    try:
        summary_event = json.dumps({
            "incident_id": builder.incident_id,
            "severity": severity,
            "node_count": node_count,
            "root_cause_node_id": root_node_id,
            "llm_summary": llm_summary,
            "closed_at": now.isoformat(),
        })
        producer.produce(
            TOPIC_OUT,
            key=builder.incident_id.encode(),
            value=summary_event.encode(),
        )
        producer.flush()
    except Exception as e:
        log.warning("Failed to publish incident to Kafka: %s", e)


def run_kafka_consumer(producer: Producer) -> None:
    consumer = Consumer({
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "group.id": GROUP_ID,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": True,
    })
    consumer.subscribe([TOPIC_IN])
    log.info("Subscribed to %s", TOPIC_IN)

    while _running:
        msg = consumer.poll(timeout=1.0)
        if msg is None:
            continue
        if msg.error():
            log.error("Consumer error: %s", msg.error())
            continue
        try:
            event = json.loads(msg.value().decode())
            node = anomaly_event_to_node(event)
            if node:
                _add_node_to_graph(node, producer)
        except Exception as e:
            log.warning("Failed to process anomaly event: %s", e)

    consumer.close()


def run_closer(producer: Producer) -> None:
    """Closes idle incidents every 30s."""
    global _builder
    while _running:
        time.sleep(30)
        try:
            with _lock:
                b = _builder
                if (
                    b.incident_id is not None
                    and b.last_event_at is not None
                    and (datetime.now(timezone.utc) - b.last_event_at).total_seconds()
                    > INCIDENT_IDLE_TIMEOUT
                ):
                    _builder = CausalGraphBuilder()  # reset before closing
                    close_target = b
                else:
                    close_target = None
            if close_target is not None:
                _close_incident(close_target, producer)
        except Exception as e:
            log.error("Closer error: %s", e)


def run_trust_drop_poller(producer: Producer) -> None:
    while _running:
        time.sleep(POLL_INTERVAL)
        try:
            conn = get_metrics_conn()
            for dataset_id in DATASET_IDS:
                scores = get_last_trust_scores(conn, dataset_id)
                if len(scores) < 2:
                    continue
                curr, prev = scores[0], scores[1]
                drop = prev - curr
                if drop > TRUST_DROP_THRESHOLD:
                    node = make_trust_drop_node(dataset_id, drop, prev, curr)
                    _add_node_to_graph(node, producer)
                    log.info("TrustDrop: %s drop=%.3f", dataset_id, drop)
            conn.close()
        except Exception as e:
            log.error("TrustDrop poller error: %s", e)


def run_prediction_drift_poller(producer: Producer) -> None:
    global _last_drift_flag_id
    while _running:
        time.sleep(POLL_INTERVAL)
        try:
            conn = get_meta_conn()
            flags = get_new_drift_flags(conn, _last_drift_flag_id)
            for flag in flags:
                node = make_prediction_drift_node(flag["event_type"], flag["ks_stat"], flag["id"])
                _add_node_to_graph(node, producer)
                _last_drift_flag_id = max(_last_drift_flag_id, flag["id"])
                log.info("PredictionDrift: %s flag_id=%d", flag["event_type"], flag["id"])
            conn.close()
        except Exception as e:
            log.error("PredictionDrift poller error: %s", e)


def run_kafka_lag_poller(producer: Producer) -> None:
    while _running:
        time.sleep(POLL_INTERVAL)
        try:
            from confluent_kafka import TopicPartition
            check_consumer = Consumer({
                "bootstrap.servers": KAFKA_BOOTSTRAP,
                "group.id": "nexus-anomaly-detector",
                "auto.offset.reset": "earliest",
            })
            partitions = [TopicPartition("nexus.raw.events", p) for p in range(3)]
            committed = check_consumer.committed(partitions, timeout=5.0)
            total_lag = 0
            for tp in committed:
                if tp.offset < 0:
                    continue
                lo, hi = check_consumer.get_watermark_offsets(tp, timeout=5.0, cached=False)
                total_lag += max(0, hi - tp.offset)
            check_consumer.close()
            if total_lag > KAFKA_LAG_THRESHOLD:
                node = make_kafka_lag_node(total_lag)
                _add_node_to_graph(node, producer)
                log.info("KafkaLag: lag=%d", total_lag)
        except Exception as e:
            log.error("KafkaLag poller error: %s", e)


def start_background_threads(producer: Producer) -> None:
    threads = [
        threading.Thread(target=run_kafka_consumer,         args=(producer,), daemon=True, name="kafka-consumer"),
        threading.Thread(target=run_closer,                  args=(producer,), daemon=True, name="incident-closer"),
        threading.Thread(target=run_trust_drop_poller,       args=(producer,), daemon=True, name="trust-drop-poll"),
        threading.Thread(target=run_prediction_drift_poller, args=(producer,), daemon=True, name="drift-poll"),
        threading.Thread(target=run_kafka_lag_poller,        args=(producer,), daemon=True, name="lag-poll"),
    ]
    for t in threads:
        t.start()
    log.info("Started %d background threads", len(threads))
