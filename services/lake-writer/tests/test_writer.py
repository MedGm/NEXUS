import time
from unittest.mock import Mock
from src.writer import LakeWriter, minio_path


def test_minio_path_format():
    # timestamp for 2026-05-27 14:30:00 UTC
    ts_ms = 1779892200000
    path = minio_path(ts_ms)
    assert path.startswith("year=2026/month=05/day=27/hour=14/")
    assert path.endswith(".jsonl")


def test_lake_writer_buffers_records():
    mock_s3 = Mock()
    writer = LakeWriter(mock_s3, flush_seconds=999)
    writer.add({"event_id": "a", "timestamp": 1748355000000})
    writer.add({"event_id": "b", "timestamp": 1748355001000})
    mock_s3.put_object.assert_not_called()


def test_lake_writer_flush_writes_jsonl_to_correct_bucket():
    mock_s3 = Mock()
    writer = LakeWriter(mock_s3, flush_seconds=999)
    writer.add({"event_id": "a", "timestamp": 1748355000000})
    writer.add({"event_id": "b", "timestamp": 1748355001000})

    count = writer.flush()

    assert count == 2
    mock_s3.put_object.assert_called_once()
    call_kwargs = mock_s3.put_object.call_args.kwargs
    assert call_kwargs["Bucket"] == "raw-data"
    body = call_kwargs["Body"].decode()
    lines = body.strip().split("\n")
    assert len(lines) == 2
    import json
    assert json.loads(lines[0])["event_id"] == "a"


def test_lake_writer_flush_empty_buffer_does_not_call_s3():
    mock_s3 = Mock()
    writer = LakeWriter(mock_s3, flush_seconds=999)
    count = writer.flush()
    assert count == 0
    mock_s3.put_object.assert_not_called()


def test_lake_writer_buffer_cleared_after_flush():
    mock_s3 = Mock()
    writer = LakeWriter(mock_s3, flush_seconds=999)
    writer.add({"event_id": "a", "timestamp": 1748355000000})
    writer.flush()
    writer.flush()
    assert mock_s3.put_object.call_count == 1


def test_lake_writer_should_flush_false_before_deadline():
    mock_s3 = Mock()
    writer = LakeWriter(mock_s3, flush_seconds=999)
    assert writer.should_flush() is False


def test_lake_writer_should_flush_true_after_deadline():
    mock_s3 = Mock()
    writer = LakeWriter(mock_s3, flush_seconds=0)
    time.sleep(0.01)
    assert writer.should_flush() is True
