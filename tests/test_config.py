import os
import pytest
import tempfile
import yaml
from migrator.config import load_config, MigratorConfig


def test_defaults_with_no_file():
    cfg = load_config("/nonexistent/path.yaml")
    assert cfg.pg.host == "localhost"
    assert cfg.pg.port == 5432
    assert cfg.scylla.keyspace == "thingsboard"
    assert cfg.batch_size == 5000
    assert cfg.partitioning == "MONTHS"


def test_yaml_values_loaded():
    data = {
        "pg": {"host": "pghost", "port": 5433, "db": "mydb", "user": "admin", "password": "secret"},
        "scylla": {"host": "scylla01", "port": 9043, "keyspace": "myks"},
        "migrator": {"batch_size": 1000, "partitioning": "DAYS", "cast_strings": True},
    }
    with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
        yaml.dump(data, f)
        path = f.name
    try:
        cfg = load_config(path)
        assert cfg.pg.host == "pghost"
        assert cfg.pg.port == 5433
        assert cfg.scylla.host == "scylla01"
        assert cfg.batch_size == 1000
        assert cfg.partitioning == "DAYS"
        assert cfg.cast_strings is True
    finally:
        os.unlink(path)


def test_env_vars_override_yaml(monkeypatch):
    monkeypatch.setenv("PG_HOST", "env-pghost")
    monkeypatch.setenv("PG_PORT", "5999")
    monkeypatch.setenv("SCYLLA_HOST", "env-scylla")
    cfg = load_config("/nonexistent/path.yaml")
    assert cfg.pg.host == "env-pghost"
    assert cfg.pg.port == 5999
    assert cfg.scylla.host == "env-scylla"
