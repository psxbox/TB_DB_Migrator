import uuid
import pytest
from unittest.mock import MagicMock, patch, call
from migrator.scylla_writer import ScyllaWriter, SCHEMA_STATEMENTS
from cassandra import WriteTimeout


@pytest.fixture
def mock_session():
    return MagicMock()


@pytest.fixture
def writer(mock_session):
    w = ScyllaWriter(mock_session, "thingsboard")
    # Pre-populate prepared statements so prepare() isn't called in every test
    w._ps_ts = MagicMock()
    w._ps_partition = MagicMock()
    w._ps_latest = MagicMock()
    return w


def test_schema_statements_count():
    """SCHEMA_STATEMENTS must have exactly 4 entries (keyspace + 3 tables)."""
    assert len(SCHEMA_STATEMENTS) == 4


def test_init_schema_executes_all_statements(mock_session):
    """init_schema executes all 4 schema statements."""
    writer = ScyllaWriter(mock_session, "thingsboard")
    writer.init_schema()
    assert mock_session.execute.call_count == 4


def test_write_ts_batch_returns_partitions(writer, mock_session):
    """write_ts_batch returns set of (entity_type, eid_str, key_name, partition) tuples."""
    rows = [
        {"entity_id": "550e8400-e29b-11d4-a716-446655440000", "key": 1,
         "ts": 1718500000000, "bool_v": None, "str_v": None,
         "long_v": 42, "dbl_v": None, "json_v": None},
    ]
    key_map = {1: "temperature"}
    partition_fn = lambda ts: 1718444800000  # fixed for test

    result = writer.write_ts_batch(rows, "DEVICE", key_map, partition_fn)

    assert len(result) == 1
    entity_type, eid_str, key_name, partition = next(iter(result))
    assert entity_type == "DEVICE"
    assert key_name == "temperature"
    assert partition == 1718444800000
    mock_session.execute.assert_called_once()


def test_write_ts_batch_retries_on_write_timeout(writer, mock_session):
    """write_ts_batch halves rows and retries on WriteTimeout."""
    rows = [
        {"entity_id": "550e8400-e29b-11d4-a716-446655440000", "key": 1,
         "ts": 1000, "bool_v": None, "str_v": None, "long_v": 1, "dbl_v": None, "json_v": None},
        {"entity_id": "550e8400-e29b-11d4-a716-446655440001", "key": 1,
         "ts": 2000, "bool_v": None, "str_v": None, "long_v": 2, "dbl_v": None, "json_v": None},
    ]
    key_map = {1: "temp"}
    partition_fn = lambda ts: 0

    # Fail once, succeed on second attempt
    mock_session.execute.side_effect = [WriteTimeout("timeout", write_type=0), None]

    with patch("time.sleep"):
        result = writer.write_ts_batch(rows, "DEVICE", key_map, partition_fn)

    assert mock_session.execute.call_count == 2
    assert isinstance(result, set)


def test_write_partitions_executes_batch(writer, mock_session):
    """write_partitions writes to ts_kv_partitions_cf."""
    partitions = {
        ("DEVICE", "550e8400-e29b-11d4-a716-446655440000", "temperature", 1718444800000),
    }
    writer.write_partitions(partitions)
    mock_session.execute.assert_called_once()


def test_write_latest_batch_executes_batch(writer, mock_session):
    """write_latest_batch writes to ts_kv_latest_cf."""
    rows = [
        {"entity_id": "550e8400-e29b-11d4-a716-446655440000", "key": 1,
         "ts": 9000, "bool_v": None, "str_v": None,
         "long_v": 99, "dbl_v": None, "json_v": None},
    ]
    key_map = {1: "temperature"}
    writer.write_latest_batch(rows, "DEVICE", key_map)
    mock_session.execute.assert_called_once()


def test_write_ts_batch_cast_strings(writer, mock_session):
    """write_ts_batch applies cast_fn to str_v when provided."""
    rows = [
        {"entity_id": "550e8400-e29b-11d4-a716-446655440000", "key": 1,
         "ts": 1000, "bool_v": None, "str_v": "42",
         "long_v": None, "dbl_v": None, "json_v": None},
    ]
    key_map = {1: "count"}
    partition_fn = lambda ts: 0

    cast_calls = []
    def cast_fn(v):
        cast_calls.append(v)
        return ("long_v", 42)

    writer.write_ts_batch(rows, "DEVICE", key_map, partition_fn, cast_fn=cast_fn)

    assert cast_calls == ["42"]
    mock_session.execute.assert_called_once()


def test_execute_with_retry_retries_on_write_timeout(writer, mock_session):
    """_execute_with_retry retries up to max_retries times on WriteTimeout."""
    mock_session.execute.side_effect = [
        WriteTimeout("timeout", write_type=0),
        WriteTimeout("timeout", write_type=0),
        None,
    ]
    with patch("time.sleep"):
        writer._execute_with_retry("SELECT 1", [], max_retries=3)
    assert mock_session.execute.call_count == 3
