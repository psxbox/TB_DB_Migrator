import pytest
from unittest.mock import MagicMock, patch, call
from migrator.pg_reader import PgReader, ENTITY_TABLES


@pytest.fixture
def mock_conn():
    conn = MagicMock()
    return conn


def make_cursor_cm(rows, cursor_factory=None):
    """Helper: returns a context manager that yields a cursor returning rows."""
    cur = MagicMock()
    cur.fetchall.return_value = rows
    cur.fetchone.return_value = rows[0] if rows else None
    cur.__iter__ = lambda self: iter(rows)
    cm = MagicMock()
    cm.__enter__ = MagicMock(return_value=cur)
    cm.__exit__ = MagicMock(return_value=False)
    return cm, cur


def test_load_entity_map_all_tables(mock_conn):
    """load_entity_map queries all 18 entity tables and returns uuid→type dict."""
    tables = list(ENTITY_TABLES.keys())
    call_index = {"n": 0}

    def fetchall_side_effect():
        idx = call_index["n"]
        call_index["n"] += 1
        table = tables[idx] if idx < len(tables) else "unknown"
        return [(f"uuid-{table}-1",), (f"uuid-{table}-2",)]

    cur = MagicMock()
    cur.fetchall.side_effect = fetchall_side_effect
    cm = MagicMock()
    cm.__enter__.return_value = cur
    cm.__exit__.return_value = False
    mock_conn.cursor.return_value = cm

    reader = PgReader(mock_conn)
    result = reader.load_entity_map()

    # Should have queried each of the 18 tables
    executed_queries = [str(c) for c in cur.execute.call_args_list]
    for table in ENTITY_TABLES:
        assert any(table in q for q in executed_queries), f"Missing query for table {table}"

    # Each table returned 2 rows, 18 tables = 36 entries
    assert len(result) == 36
    assert result["uuid-device-1"] == "DEVICE"


def test_load_key_map(mock_conn):
    """load_key_map returns {key_id: key_name} dict."""
    cur = MagicMock()
    cur.fetchall.return_value = [(1, "temperature"), (2, "humidity")]
    cm = MagicMock()
    cm.__enter__.return_value = cur
    cm.__exit__.return_value = False
    mock_conn.cursor.return_value = cm

    reader = PgReader(mock_conn)
    result = reader.load_key_map()

    assert result == {1: "temperature", 2: "humidity"}
    cur.execute.assert_called_once_with("SELECT key_id, key FROM ts_kv_dictionary")


def test_iter_ts_kv_for_entity_single_batch(mock_conn):
    """iter_ts_kv_for_entity yields rows and stops when batch is partial."""
    rows = [
        {"entity_id": "e1", "key": 1, "ts": 1000, "bool_v": None, "str_v": None,
         "long_v": 42, "dbl_v": None, "json_v": None},
    ]
    cur = MagicMock()
    cur.fetchall.return_value = rows
    cm = MagicMock()
    cm.__enter__.return_value = cur
    cm.__exit__.return_value = False
    mock_conn.cursor.return_value = cm

    reader = PgReader(mock_conn)
    result = list(reader.iter_ts_kv_for_entity("e1", batch_size=5000))

    assert len(result) == 1
    assert result[0]["long_v"] == 42


def test_iter_ts_kv_by_ts_yields_rows(mock_conn):
    """iter_ts_kv_by_ts yields rows with ts > watermark and stops on partial batch."""
    rows = [
        {"entity_id": "e1", "key": 2, "ts": 2000, "bool_v": None, "str_v": "hi",
         "long_v": None, "dbl_v": None, "json_v": None},
    ]
    cur = MagicMock()
    cur.fetchall.return_value = rows
    cm = MagicMock()
    cm.__enter__.return_value = cur
    cm.__exit__.return_value = False
    mock_conn.cursor.return_value = cm

    reader = PgReader(mock_conn)
    result = list(reader.iter_ts_kv_by_ts(watermark_ts=1000, batch_size=5000))

    assert len(result) == 1
    assert result[0]["str_v"] == "hi"
    # Verify watermark was passed
    call_args = cur.execute.call_args
    assert 1000 in call_args[0][1]
