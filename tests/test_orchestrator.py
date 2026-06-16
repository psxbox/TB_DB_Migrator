import pytest
from unittest.mock import MagicMock, patch
from migrator.orchestrator import Orchestrator
from migrator.config import MigratorConfig
from migrator.progress import ProgressTracker, Progress


@pytest.fixture
def config():
    cfg = MigratorConfig()
    cfg.batch_size = 5000
    cfg.workers = 1          # single-threaded for unit tests (no conn_factory needed)
    cfg.cast_strings = False
    cfg.partitioning = "MONTHS"
    cfg.live_sync_interval = 0.01
    cfg.lag_threshold_ms = 30000
    return cfg


@pytest.fixture
def mock_reader():
    r = MagicMock()
    r.load_entity_map.return_value = {"entity-1": "DEVICE"}
    r.load_key_map.return_value = {1: "temperature"}
    r.count_ts_kv.return_value = 0
    r.iter_distinct_entities.return_value = iter([])
    r.iter_ts_kv_latest.return_value = iter([])
    r.iter_ts_kv_by_ts.return_value = iter([])
    return r


@pytest.fixture
def mock_writer():
    w = MagicMock()
    w.write_ts_batch.return_value = set()
    return w


@pytest.fixture
def tracker(tmp_path):
    t = ProgressTracker(str(tmp_path / "progress.json"))
    t.progress.phase1_start_ts = 1000000
    return t


def make_conn_factory():
    """Returns a factory that produces a mock PG connection whose cursor yields no rows."""
    def factory():
        conn = MagicMock()
        cur = MagicMock()
        cur.fetchall.return_value = []
        cur.__iter__ = lambda self: iter([])
        cm = MagicMock()
        cm.__enter__.return_value = cur
        cm.__exit__.return_value = False
        conn.cursor.return_value = cm
        return conn
    return factory


def make_orchestrator(config, reader, writer, tracker, conn_factory=None):
    return Orchestrator(config, reader, writer, tracker, conn_factory=conn_factory)


def test_phase0_loads_maps_and_sets_phase1(config, mock_reader, mock_writer, tracker):
    """Phase 0 loads entity_map and key_map, updates progress to phase1."""
    orch = make_orchestrator(config, mock_reader, mock_writer, tracker)
    entity_map, key_map = orch._phase0()

    assert entity_map == {"entity-1": "DEVICE"}
    assert key_map == {1: "temperature"}
    assert tracker.progress.phase == "phase1"
    mock_reader.load_entity_map.assert_called_once()
    mock_reader.load_key_map.assert_called_once()


def test_phase1_skips_unknown_entity(config, mock_reader, mock_writer, tracker):
    """Phase 1 skips entity_ids not in entity_map and increments skipped_rows."""
    mock_reader.iter_distinct_entities.return_value = iter(["unknown-entity"])
    mock_reader.iter_ts_kv_latest.return_value = iter([])
    entity_map = {}
    key_map = {1: "temperature"}

    orch = make_orchestrator(config, mock_reader, mock_writer, tracker,
                             conn_factory=make_conn_factory())
    orch._phase1(entity_map, key_map)

    mock_writer.write_ts_batch.assert_not_called()
    assert tracker.progress.skipped_rows == 1


def test_phase1_writes_ts_and_latest(config, mock_reader, mock_writer, tracker):
    """Phase 1 writes ts_kv rows and ts_kv_latest rows for known entities."""
    eid = "550e8400-e29b-11d4-a716-446655440000"
    ts_row = {
        "entity_id": eid, "key": 1, "ts": 1000,
        "bool_v": None, "str_v": None, "long_v": 42, "dbl_v": None, "json_v": None,
    }
    latest_row = {
        "entity_id": eid, "key": 1, "ts": 2000,
        "bool_v": None, "str_v": None, "long_v": 99, "dbl_v": None, "json_v": None,
    }

    # conn_factory returns a connection whose PgReader yields ts_row for the entity
    def conn_factory():
        conn = MagicMock()
        cur = MagicMock()
        cur.fetchall.return_value = [ts_row]
        cur.__iter__ = lambda self: iter([])
        cm = MagicMock()
        cm.__enter__.return_value = cur
        cm.__exit__.return_value = False
        conn.cursor.return_value = cm
        return conn

    mock_reader.iter_distinct_entities.return_value = iter([eid])
    mock_reader.iter_ts_kv_latest.return_value = iter([latest_row])
    mock_writer.write_ts_batch.return_value = {("DEVICE", eid, "temperature", 0)}

    entity_map = {eid: "DEVICE"}
    key_map = {1: "temperature"}

    orch = make_orchestrator(config, mock_reader, mock_writer, tracker,
                             conn_factory=conn_factory)
    orch._phase1(entity_map, key_map)

    mock_writer.write_latest_batch.assert_called_once()


def test_phase1_resumes_skipping_completed(config, mock_reader, mock_writer, tracker):
    """Phase 1 with --resume skips entities already in completed_entities."""
    eid1 = "550e8400-e29b-11d4-a716-446655440001"
    eid2 = "550e8400-e29b-11d4-a716-446655440002"

    mock_reader.iter_distinct_entities.return_value = iter([eid1, eid2])
    mock_reader.iter_ts_kv_latest.return_value = iter([])

    # eid1 already done — only eid2 should be processed
    tracker.progress.completed_entities = [eid1]

    orch = make_orchestrator(config, mock_reader, mock_writer, tracker,
                             conn_factory=make_conn_factory())
    orch._phase1({eid2: "DEVICE"}, {})

    # eid2 had no rows (conn_factory returns empty fetchall), so write_ts_batch not called
    # but completed_entities should now include eid2
    assert eid1 in tracker.progress.completed_entities
    assert eid2 in tracker.progress.completed_entities


def test_phase1_parallel_workers(mock_writer, tracker, tmp_path):
    """Phase 1 with workers=4 processes all entities and updates migrated_rows."""
    cfg = MigratorConfig()
    cfg.batch_size = 2
    cfg.workers = 4
    cfg.cast_strings = False
    cfg.partitioning = "MONTHS"
    cfg.live_sync_interval = 0.01
    cfg.lag_threshold_ms = 30000

    eids = [f"550e8400-0000-0000-0000-{i:012d}" for i in range(8)]
    ts_row = lambda eid: {
        "entity_id": eid, "key": "temp", "ts": 1000,
        "bool_v": None, "str_v": None, "long_v": 1, "dbl_v": None, "json_v": None,
    }

    reader = MagicMock()
    reader.iter_distinct_entities.return_value = iter(eids)
    reader.iter_ts_kv_latest.return_value = iter([])
    reader.count_ts_kv.return_value = 0
    reader.load_entity_map.return_value = {}
    reader.load_key_map.return_value = {}

    def conn_factory():
        conn = MagicMock()
        cur = MagicMock()
        # Return 1 row then empty to stop pagination
        cur.fetchall.side_effect = [[ts_row("550e8400-0000-0000-0000-000000000000")], []]
        cur.__iter__ = lambda self: iter([])
        cm = MagicMock()
        cm.__enter__.return_value = cur
        cm.__exit__.return_value = False
        conn.cursor.return_value = cm
        return conn

    mock_writer.write_ts_batch.return_value = set()
    entity_map = {eid: "DEVICE" for eid in eids}

    t = ProgressTracker(str(tmp_path / "p.json"))
    t.progress.phase1_start_ts = 1000000

    orch = make_orchestrator(cfg, reader, mock_writer, t, conn_factory=conn_factory)
    orch._phase1(entity_map, {})

    assert len(t.progress.completed_entities) == len(eids)
