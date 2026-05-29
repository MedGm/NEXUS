import os
from confluent_kafka import Producer, Consumer
from confluent_kafka.schema_registry import SchemaRegistryClient


def make_schema_registry_client(url: str | None = None) -> SchemaRegistryClient:
    return SchemaRegistryClient({"url": url or os.environ["SCHEMA_REGISTRY_URL"]})


def make_producer(bootstrap: str | None = None) -> Producer:
    return Producer({
        "bootstrap.servers": bootstrap or os.environ["KAFKA_BOOTSTRAP_SERVERS"],
        "acks": "all",
    })


def make_consumer(
    bootstrap: str | None = None,
    group_id: str = "nexus-consumer",
) -> Consumer:
    return Consumer({
        "bootstrap.servers": bootstrap or os.environ["KAFKA_BOOTSTRAP_SERVERS"],
        "group.id": group_id,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    })
