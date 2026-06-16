import pytest
from unittest.mock import MagicMock, patch, call
from migrator.orchestrator import Orchestrator
from migrator.config import MigratorConfig
from migrator.progress import ProgressTracker, Progress


@pytest.fixture
def config():
    cfg = MigratorConfig()
    cfg.batch_size = 5000
    cfg.cast_strings = False
    cfg.partitioning = "MONTHS"
    cfg.live_sync_interval = 0.01  # fast for tests
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


def make_orchestrator(config, reader, writer, tracker):
    return Orchestrator(config, reader, writer, tracker)


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
    entity_map = {}  # empty — unknown-entity will be skipped
    key_map = {1: "temperature"}

    orch = make_orchestrator(config, mock_reader, mock_writer, tracker)
    orch._phase1(entity_map, key_map)

    mock_writer.write_ts_batch.assert_not_called()
    assert tracker.progress.skipped_rows == 1


def test_phase1_writes_ts_and_latest(config, mock_reader, mock_writer, tracker):
    """Phase 1 writes ts_kv rows and ts_kv_latest rows for known entities."""
    ts_row = {
        "entity_id": "550e8400-e29b-11d4-a716-446655440000",
        "key": 1, "ts": 1000,
        "bool_v": None, "str_v": None, "long_v": 42, "dbl_v": None, "json_v": None,
    }
    latest_row = {
        "entity_id": "550e8400-e29b-11d4-a716-446655440000",
        "key": 1, "ts": 2000,
        "bool_v": None, "str_v": None, "long_v": 99, "dbl_v": None, "json_v": None,
    }
    mock_reader.iter_distinct_entities.return_value = iter(["550e8400-e29b-11d4-a716-446655440000"])
    mock_reader.iter_ts_kv_for_entity.return_value = iter([ts_row])
    mock_reader.iter_ts_kv_latest.return_value = iter([latest_row])
    mock_writer.write_ts_batch.return_value = {("DEVICE", "550e8400-e29b-11d4-a716-446655440000", "temperature", 0)}

    entity_map = {"550e8400-e29b-11d4-a716-446655440000": "DEVICE"}
    key_map = {1: "temperature"}

    orch = make_orchestrator(config, mock_reader, mock_writer, tracker)
    orch._phase1(entity_map, key_map)

    mock_writer.write_ts_batch.assert_called_once()
    mock_writer.write_partitions.assert_called_once()
    mock_writer.write_latest_batch.assert_called_once()
