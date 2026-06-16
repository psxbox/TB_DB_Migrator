import uuid
import pytest
from unittest.mock import MagicMock, patch
from migrator.scylla_writer import ScyllaWriter, SCHEMA_STATEMENTS


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


@patch("migrator.scylla_writer.execute_concurrent_with_args")
def test_write_ts_batch_returns_partitions(mock_ec, writer):
    """write_ts_batch returns set of (entity_type, eid_str, key_name, partition) tuples."""
    mock_ec.return_value = [(True, None)]
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
    mock_ec.assert_called_once()


@patch("migrator.scylla_writer.time.sleep")
@patch("migrator.scylla_writer.execute_concurrent_with_args")
def test_execute_concurrent_retries_failed_rows(mock_ec, mock_sleep, writer):
    """_execute_concurrent retries only the rows that failed, then succeeds."""
    err = Exception("write timeout")
    # First call: the single row fails. Second call: it succeeds.
    mock_ec.side_effect = [
        [(False, err)],
        [(True, None)],
    ]
    writer._execute_concurrent(writer._ps_ts, [("a",)], max_retries=4)
    assert mock_ec.call_count == 2


@patch("migrator.scylla_writer.time.sleep")
@patch("migrator.scylla_writer.execute_concurrent_with_args")
def test_execute_concurrent_raises_after_retries(mock_ec, mock_sleep, writer):
    """_execute_concurrent raises the underlying error once retries are exhausted."""
    err = RuntimeError("permanent failure")
    mock_ec.return_value = [(False, err)]
    with pytest.raises(RuntimeError, match="permanent failure"):
        writer._execute_concurrent(writer._ps_ts, [("a",)], max_retries=2)
    assert mock_ec.call_count == 2


@patch("migrator.scylla_writer.execute_concurrent_with_args")
def test_write_partitions_executes(mock_ec, writer):
    """write_partitions writes to ts_kv_partitions_cf."""
    mock_ec.return_value = [(True, None)]
    partitions = {
        ("DEVICE", "550e8400-e29b-11d4-a716-446655440000", "temperature", 1718444800000),
    }
    writer.write_partitions(partitions)
    mock_ec.assert_called_once()


@patch("migrator.scylla_writer.execute_concurrent_with_args")
def test_write_latest_batch_executes(mock_ec, writer):
    """write_latest_batch writes to ts_kv_latest_cf."""
    mock_ec.return_value = [(True, None)]
    rows = [
        {"entity_id": "550e8400-e29b-11d4-a716-446655440000", "key": 1,
         "ts": 9000, "bool_v": None, "str_v": None,
         "long_v": 99, "dbl_v": None, "json_v": None},
    ]
    key_map = {1: "temperature"}
    writer.write_latest_batch(rows, "DEVICE", key_map)
    mock_ec.assert_called_once()


@patch("migrator.scylla_writer.execute_concurrent_with_args")
def test_write_ts_batch_cast_strings(mock_ec, writer):
    """write_ts_batch applies cast_fn to str_v when provided."""
    mock_ec.return_value = [(True, None)]
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
    mock_ec.assert_called_once()


@patch("migrator.scylla_writer.execute_concurrent_with_args")
def test_write_empty_batches_are_noops(mock_ec, writer):
    """Empty inputs must not call execute_concurrent."""
    writer.write_ts_batch([], "DEVICE", {}, lambda ts: 0)
    writer.write_partitions(set())
    writer.write_latest_batch([], "DEVICE", {})
    mock_ec.assert_not_called()
