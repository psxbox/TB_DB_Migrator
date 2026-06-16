import os
import json
import tempfile
import pytest
from migrator.progress import ProgressTracker


@pytest.fixture
def tmp_checkpoint(tmp_path):
    return str(tmp_path / "progress.json")


def test_load_returns_false_when_no_file(tmp_checkpoint):
    tracker = ProgressTracker(tmp_checkpoint)
    assert tracker.load() is False


def test_save_and_load_roundtrip(tmp_checkpoint):
    tracker = ProgressTracker(tmp_checkpoint)
    tracker.progress.phase = "phase1"
    tracker.progress.migrated_rows = 12345
    tracker.progress.last_entity_id = "abc-123"
    tracker.save()

    tracker2 = ProgressTracker(tmp_checkpoint)
    assert tracker2.load() is True
    assert tracker2.progress.phase == "phase1"
    assert tracker2.progress.migrated_rows == 12345
    assert tracker2.progress.last_entity_id == "abc-123"


def test_update_saves_immediately(tmp_checkpoint):
    tracker = ProgressTracker(tmp_checkpoint)
    tracker.update(phase="live_sync", watermark_ts=999999)
    assert tracker.progress.phase == "live_sync"
    assert tracker.progress.watermark_ts == 999999
    with open(tmp_checkpoint) as f:
        data = json.load(f)
    assert data["phase"] == "live_sync"
    assert data["watermark_ts"] == 999999
