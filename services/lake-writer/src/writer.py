import json
import time
from datetime import datetime, timezone


def minio_path(event_timestamp_ms: int) -> str:
    dt = datetime.fromtimestamp(event_timestamp_ms / 1000, tz=timezone.utc)
    filename = f"{int(time.time() * 1000)}.jsonl"
    return (
        f"year={dt.year}/month={dt.month:02d}/"
        f"day={dt.day:02d}/hour={dt.hour:02d}/{filename}"
    )


class LakeWriter:
    def __init__(self, s3_client, bucket: str = "raw-data", flush_seconds: int = 60):
        self._s3 = s3_client
        self._bucket = bucket
        self._flush_seconds = flush_seconds
        self._buffer: list[dict] = []
        self._flush_at = time.monotonic() + flush_seconds

    def add(self, record: dict) -> None:
        self._buffer.append(record)

    def should_flush(self) -> bool:
        return time.monotonic() >= self._flush_at

    def flush(self) -> int:
        self._flush_at = time.monotonic() + self._flush_seconds
        if not self._buffer:
            return 0

        ts = self._buffer[0].get("timestamp", int(time.time() * 1000))
        path = minio_path(ts)
        body = "\n".join(json.dumps(r) for r in self._buffer).encode()

        self._s3.put_object(Bucket=self._bucket, Key=path, Body=body)

        count = len(self._buffer)
        self._buffer.clear()
        return count
