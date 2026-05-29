import os
import signal
import logging
import boto3

from nexus_common.kafka import make_schema_registry_client, make_consumer
from nexus_common.avro import get_deserializer, deserialize
from .writer import LakeWriter

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(message)s")
log = logging.getLogger(__name__)

TOPIC = "nexus.raw.events"
GROUP_ID = "nexus-lake-writer"
FLUSH_SECONDS = int(os.environ.get("LAKE_WRITER_FLUSH_SECONDS", "60"))

_running = True


def _stop(sig, frame):
    global _running
    _running = False


signal.signal(signal.SIGTERM, _stop)
signal.signal(signal.SIGINT, _stop)


def main() -> None:
    sr = make_schema_registry_client()
    deserializer = get_deserializer(sr)
    consumer = make_consumer(group_id=GROUP_ID)
    consumer.subscribe([TOPIC])

    s3 = boto3.client(
        "s3",
        endpoint_url=os.environ["MINIO_ENDPOINT"],
        aws_access_key_id=os.environ["MINIO_ACCESS_KEY"],
        aws_secret_access_key=os.environ["MINIO_SECRET_KEY"],
    )

    writer = LakeWriter(s3, flush_seconds=FLUSH_SECONDS)

    while _running:
        msg = consumer.poll(timeout=1.0)
        if msg is None:
            pass
        elif msg.error():
            log.error("Consumer error: %s", msg.error())
        else:
            record = deserialize(deserializer, TOPIC, msg.value())
            if record is not None:
                writer.add(record)

        if writer.should_flush():
            count = writer.flush()
            if count:
                log.info("Flushed %d records to MinIO", count)
            consumer.commit(asynchronous=False)

    count = writer.flush()
    if count:
        log.info("Final flush: %d records", count)
    consumer.close()


if __name__ == "__main__":
    main()
