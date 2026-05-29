from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer, AvroDeserializer
from confluent_kafka.serialization import SerializationContext, MessageField


def _topic_record_name_strategy(ctx: SerializationContext, record_name: str) -> str:
    return f"{ctx.topic}-{record_name}"


def get_serializer(schema_path: str, sr_client: SchemaRegistryClient) -> AvroSerializer:
    with open(schema_path) as f:
        schema_str = f.read()
    return AvroSerializer(
        sr_client,
        schema_str,
        conf={
            "auto.register.schemas": True,
            "subject.name.strategy": _topic_record_name_strategy,
        },
    )


def get_deserializer(sr_client: SchemaRegistryClient) -> AvroDeserializer:
    return AvroDeserializer(sr_client)


def serialize(serializer: AvroSerializer, topic: str, value: dict) -> bytes | None:
    ctx = SerializationContext(topic, MessageField.VALUE)
    return serializer(value, ctx)


def deserialize(deserializer: AvroDeserializer, topic: str, data: bytes) -> dict | None:
    ctx = SerializationContext(topic, MessageField.VALUE)
    return deserializer(data, ctx)
