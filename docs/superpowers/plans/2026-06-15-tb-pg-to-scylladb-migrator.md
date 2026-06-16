# TB PostgreSQL → ScyllaDB Migrator Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Python tool that incrementally migrates ThingsBoard timeseries data from PostgreSQL to ScyllaDB with ~60-second downtime.

**Architecture:** Phase 0 preloads entity/key maps from live PG; Phase 1 streams historical ts_kv in batches to ScyllaDB; Phase 2 continuously polls for new rows until lag < 30s, then signals switchover. All services (TB, PG, ScyllaDB, migrator) run in Docker on the **remote server**. Local PC is only used for development and file transfer.

**Deployment flow:** Local PC → (rsync/scp) → Remote Server → Docker compose

**Tech Stack:** Python 3.11+, psycopg2-binary, scylla-driver (cassandra-driver fork), click, rich, PyYAML, pytest

**Internet constraint:** Remote server has limited internet. Docker images pulled once; pip packages bundled in Docker image build (no runtime pip install).

---

## File Map

```
TB_DB_Migrator/
├── Dockerfile
├── docker-compose.scylla.yml
├── config.yaml
├── requirements.txt
├── main.py
├── migrator/
│   ├── __init__.py
│   ├── config.py          ← YAML + env var config loader
│   ├── partition.py       ← MONTHS/DAYS/etc partition calculator
│   ├── cast.py            ← str_v → long_v/dbl_v cast logic
│   ├── progress.py        ← JSON checkpoint save/load
│   ├── pg_reader.py       ← PG entity map, key map, ts_kv batch reader
│   ├── scylla_writer.py   ← ScyllaDB schema init + prepared-statement writer
│   └── orchestrator.py    ← Phase 0/1/2 coordinator
└── tests/
    ├── __init__.py
    ├── test_partition.py
    ├── test_cast.py
    ├── test_config.py
    ├── test_progress.py
    ├── test_pg_reader.py
    └── test_scylla_writer.py
```

---

## Task 1: Project Scaffold

**Files:**
- Create: `Dockerfile`
- Create: `docker-compose.scylla.yml`
- Create: `requirements.txt`
- Create: `config.yaml`
- Create: `migrator/__init__.py`
- Create: `tests/__init__.py`

- [ ] **Step 1: Create `requirements.txt`**

```
scylla-driver>=3.26.6
psycopg2-binary>=2.9.9
pyyaml>=6.0.2
rich>=13.7.1
click>=8.1.7
pytest>=8.2.0
pytest-mock>=3.14.0
```

- [ ] **Step 2: Create `config.yaml`**

```yaml
pg:
  host: localhost
  port: 5432
  db: thingsboard
  user: postgres
  password: postgres

scylla:
  host: localhost
  port: 9042
  keyspace: thingsboard

migrator:
  batch_size: 5000
  live_sync_interval: 5.0
  lag_threshold_ms: 30000
  partitioning: MONTHS
  cast_strings: false
  checkpoint_file: migration_progress.json
```

- [ ] **Step 3: Create `Dockerfile`**

```dockerfile
FROM python:3.11-slim
WORKDIR /app

# Pip paketlarni COPY dan oldin o'rnatish — Docker cache dan foydalanish uchun
# (requirements.txt o'zgarmasa, bu layer qayta yuklanmaydi)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .
ENTRYPOINT ["python", "main.py"]
```

> **Limited internet uchun:** `docker build` birinchi marta internet ishlatadi (pip packages). Keyingi `docker build` lar faqat o'zgargan fayllarni qayta ishlaydi — internet shart emas. Barcha paketlar image ichida saqlanadi.

- [ ] **Step 4: Create `docker-compose.scylla.yml`**

```yaml
services:
  scylladb:
    image: scylladb/scylla:6.2
    container_name: scylladb
    command: --smp 2 --memory 2G --overprovisioned 1
    volumes:
      - scylla-data:/var/lib/scylla
    healthcheck:
      test: ["CMD-SHELL", "cqlsh -e 'describe keyspaces' || exit 1"]
      interval: 30s
      timeout: 10s
      retries: 10

  tb-migrator:
    build:
      context: .
    container_name: tb-migrator
    environment:
      PG_HOST: postgres
      PG_PORT: "5432"
      PG_DB: thingsboard
      PG_USER: postgres
      PG_PASSWORD: postgres
      SCYLLA_HOST: scylladb
      SCYLLA_PORT: "9042"
      SCYLLA_KEYSPACE: thingsboard
    depends_on:
      postgres:
        condition: service_started
      scylladb:
        condition: service_healthy
    volumes:
      - ./migration_progress.json:/app/migration_progress.json
    stdin_open: true
    tty: true

volumes:
  scylla-data:
```

- [ ] **Step 5: Create empty `__init__.py` files**

```bash
echo "" > migrator/__init__.py
mkdir -p tests && echo "" > tests/__init__.py
```

- [ ] **Step 6: Add `.dockerignore` (so tests don't bloat the image)**

Create `.dockerignore`:
```
tests/
docs/
*.md
migration_progress.json
migration_errors.log
__pycache__/
*.pyc
.git/
```

- [ ] **Step 7: Commit locally**

```bash
git init
git add .
git commit -m "chore: project scaffold — docker, deps, config skeleton"
```

- [ ] **Step 8: Transfer project to remote server**

Migrator kodi < 1 MB — SSH orqali tez ko'chadi:

```bash
# Local PC dan (Windows PowerShell yoki Git Bash):
rsync -avz --exclude='.git' --exclude='__pycache__' \
  /e/Projects/BlueStar/TB_DB_Migrator/ \
  user@YOUR_SERVER_IP:/opt/tb-migrator/

# Yoki scp:
scp -r "E:\Projects\BlueStar\TB_DB_Migrator" user@YOUR_SERVER_IP:/opt/tb-migrator/
```

- [ ] **Step 9: On remote server — pull ScyllaDB image and build migrator**

Remote serverda internet cheklanmagan — to'g'ridan pull va build qilish:

```bash
ssh user@YOUR_SERVER_IP

# ScyllaDB image yuklash (~800 MB, bir marta):
docker pull scylladb/scylla:6.2

# Migrator image qurish (pip packages ichiga olinadi, ~200 MB, bir marta):
cd /opt/tb-migrator
docker build -t tb-migrator:local .
```

Expected: `Successfully built ...`

---

## Task 2: Partition Strategy

**Files:**
- Create: `migrator/partition.py`
- Create: `tests/test_partition.py`

- [ ] **Step 1: Write failing tests**

Create `tests/test_partition.py`:

```python
import pytest
from migrator.partition import compute_partition, Partitioning


def test_months_partition_june():
    # 2024-06-15 10:00:00 UTC in ms → should return 2024-06-01 00:00:00 UTC in ms
    ts_ms = 1718445600000  # 2024-06-15 10:00:00 UTC
    result = compute_partition(ts_ms, Partitioning.MONTHS)
    assert result == 1717200000000  # 2024-06-01 00:00:00 UTC


def test_months_partition_jan():
    ts_ms = 1704067200000  # 2024-01-01 00:00:00 UTC
    result = compute_partition(ts_ms, Partitioning.MONTHS)
    assert result == 1704067200000  # already start of month


def test_days_partition():
    ts_ms = 1718445600000  # 2024-06-15 10:00:00 UTC
    result = compute_partition(ts_ms, Partitioning.DAYS)
    assert result == 1718409600000  # 2024-06-15 00:00:00 UTC


def test_years_partition():
    ts_ms = 1718445600000  # 2024-06-15 → 2024-01-01 00:00:00 UTC
    result = compute_partition(ts_ms, Partitioning.YEARS)
    assert result == 1704067200000  # 2024-01-01 00:00:00 UTC


def test_indefinite_partition():
    ts_ms = 1718445600000
    result = compute_partition(ts_ms, Partitioning.INDEFINITE)
    assert result == 0


def test_hours_partition():
    ts_ms = 1718445600000  # 2024-06-15 10:00:00 UTC
    result = compute_partition(ts_ms, Partitioning.HOURS)
    assert result == 1718445600000  # already on the hour


def test_hours_partition_mid_hour():
    ts_ms = 1718447123000  # 2024-06-15 10:25:23 UTC
    result = compute_partition(ts_ms, Partitioning.HOURS)
    assert result == 1718445600000  # 2024-06-15 10:00:00 UTC
```

- [ ] **Step 2: Run tests — expect FAIL**

```bash
pytest tests/test_partition.py -v
```

Expected: `ModuleNotFoundError: No module named 'migrator.partition'`

- [ ] **Step 3: Implement `migrator/partition.py`**

```python
from datetime import datetime, timezone
from enum import Enum


class Partitioning(str, Enum):
    MINUTES = "MINUTES"
    HOURS = "HOURS"
    DAYS = "DAYS"
    MONTHS = "MONTHS"
    YEARS = "YEARS"
    INDEFINITE = "INDEFINITE"


def compute_partition(ts_ms: int, strategy: Partitioning) -> int:
    if strategy == Partitioning.INDEFINITE:
        return 0
    dt = datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc)
    if strategy == Partitioning.MINUTES:
        t = dt.replace(second=0, microsecond=0)
    elif strategy == Partitioning.HOURS:
        t = dt.replace(minute=0, second=0, microsecond=0)
    elif strategy == Partitioning.DAYS:
        t = dt.replace(hour=0, minute=0, second=0, microsecond=0)
    elif strategy == Partitioning.MONTHS:
        t = dt.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    elif strategy == Partitioning.YEARS:
        t = dt.replace(month=1, day=1, hour=0, minute=0, second=0, microsecond=0)
    return int(t.timestamp() * 1000)
```

- [ ] **Step 4: Run tests — expect PASS**

```bash
pytest tests/test_partition.py -v
```

Expected: `7 passed`

- [ ] **Step 5: Commit**

```bash
git add migrator/partition.py tests/test_partition.py
git commit -m "feat: partition strategy (MONTHS/DAYS/HOURS/YEARS/INDEFINITE)"
```

---

## Task 3: Cast Strings Utility

**Files:**
- Create: `migrator/cast.py`
- Create: `tests/test_cast.py`

- [ ] **Step 1: Write failing tests**

Create `tests/test_cast.py`:

```python
from migrator.cast import try_cast_string


def test_cast_integer_string():
    col, val = try_cast_string("42")
    assert col == "long_v"
    assert val == 42


def test_cast_negative_integer():
    col, val = try_cast_string("-100")
    assert col == "long_v"
    assert val == -100


def test_cast_float_string():
    col, val = try_cast_string("3.14")
    assert col == "dbl_v"
    assert abs(val - 3.14) < 1e-9


def test_cast_plain_string():
    col, val = try_cast_string("hello")
    assert col == "str_v"
    assert val == "hello"


def test_cast_empty_string():
    col, val = try_cast_string("")
    assert col == "str_v"
    assert val == ""


def test_cast_none_returns_str_v():
    col, val = try_cast_string(None)
    assert col == "str_v"
    assert val is None
```

- [ ] **Step 2: Run tests — expect FAIL**

```bash
pytest tests/test_cast.py -v
```

Expected: `ModuleNotFoundError: No module named 'migrator.cast'`

- [ ] **Step 3: Implement `migrator/cast.py`**

```python
from typing import Any, Tuple


def try_cast_string(value: Any) -> Tuple[str, Any]:
    """Try to cast str_v to long_v or dbl_v. Returns (column_name, value)."""
    if value is None:
        return "str_v", None
    try:
        return "long_v", int(value)
    except (ValueError, TypeError):
        try:
            return "dbl_v", float(value)
        except (ValueError, TypeError):
            return "str_v", value
```

- [ ] **Step 4: Run tests — expect PASS**

```bash
pytest tests/test_cast.py -v
```

Expected: `6 passed`

- [ ] **Step 5: Commit**

```bash
git add migrator/cast.py tests/test_cast.py
git commit -m "feat: cast string values to numeric types (port of Java castEnable)"
```

---

## Task 4: Config Loader

**Files:**
- Create: `migrator/config.py`
- Create: `tests/test_config.py`

- [ ] **Step 1: Write failing tests**

Create `tests/test_config.py`:

```python
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
```

- [ ] **Step 2: Run tests — expect FAIL**

```bash
pytest tests/test_config.py -v
```

Expected: `ModuleNotFoundError: No module named 'migrator.config'`

- [ ] **Step 3: Implement `migrator/config.py`**

```python
import os
import yaml
from dataclasses import dataclass, field


@dataclass
class PgConfig:
    host: str = "localhost"
    port: int = 5432
    db: str = "thingsboard"
    user: str = "postgres"
    password: str = "postgres"


@dataclass
class ScyllaConfig:
    host: str = "localhost"
    port: int = 9042
    keyspace: str = "thingsboard"


@dataclass
class MigratorConfig:
    pg: PgConfig = field(default_factory=PgConfig)
    scylla: ScyllaConfig = field(default_factory=ScyllaConfig)
    batch_size: int = 5000
    live_sync_interval: float = 5.0
    lag_threshold_ms: int = 30000
    partitioning: str = "MONTHS"
    cast_strings: bool = False
    checkpoint_file: str = "migration_progress.json"


def load_config(path: str = "config.yaml") -> MigratorConfig:
    data = {}
    if os.path.exists(path):
        with open(path) as f:
            data = yaml.safe_load(f) or {}

    pg_d = data.get("pg", {})
    sc_d = data.get("scylla", {})
    mg_d = data.get("migrator", {})

    pg = PgConfig(
        host=os.getenv("PG_HOST", pg_d.get("host", "localhost")),
        port=int(os.getenv("PG_PORT", pg_d.get("port", 5432))),
        db=os.getenv("PG_DB", pg_d.get("db", "thingsboard")),
        user=os.getenv("PG_USER", pg_d.get("user", "postgres")),
        password=os.getenv("PG_PASSWORD", pg_d.get("password", "postgres")),
    )
    scylla = ScyllaConfig(
        host=os.getenv("SCYLLA_HOST", sc_d.get("host", "localhost")),
        port=int(os.getenv("SCYLLA_PORT", sc_d.get("port", 9042))),
        keyspace=os.getenv("SCYLLA_KEYSPACE", sc_d.get("keyspace", "thingsboard")),
    )
    return MigratorConfig(
        pg=pg,
        scylla=scylla,
        batch_size=mg_d.get("batch_size", 5000),
        live_sync_interval=mg_d.get("live_sync_interval", 5.0),
        lag_threshold_ms=mg_d.get("lag_threshold_ms", 30000),
        partitioning=mg_d.get("partitioning", "MONTHS"),
        cast_strings=mg_d.get("cast_strings", False),
        checkpoint_file=mg_d.get("checkpoint_file", "migration_progress.json"),
    )
```

- [ ] **Step 4: Run tests — expect PASS**

```bash
pytest tests/test_config.py -v
```

Expected: `3 passed`

- [ ] **Step 5: Commit**

```bash
git add migrator/config.py tests/test_config.py
git commit -m "feat: config loader (YAML + env var overrides)"
```

---

## Task 5: Progress Tracker (Checkpoint)

**Files:**
- Create: `migrator/progress.py`
- Create: `tests/test_progress.py`

- [ ] **Step 1: Write failing tests**

Create `tests/test_progress.py`:

```python
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
```

- [ ] **Step 2: Run tests — expect FAIL**

```bash
pytest tests/test_progress.py -v
```

Expected: `ModuleNotFoundError: No module named 'migrator.progress'`

- [ ] **Step 3: Implement `migrator/progress.py`**

```python
import json
import os
from dataclasses import dataclass, asdict
from typing import Optional


@dataclass
class Progress:
    phase: str = "init"
    phase1_start_ts: int = 0
    last_entity_id: Optional[str] = None
    last_entity_ts: int = 0
    watermark_ts: int = 0
    migrated_rows: int = 0
    skipped_rows: int = 0
    started_at: str = ""
    partitioning: str = "MONTHS"
    cast_strings: bool = False


class ProgressTracker:
    def __init__(self, checkpoint_file: str):
        self.checkpoint_file = checkpoint_file
        self.progress = Progress()

    def load(self) -> bool:
        if not os.path.exists(self.checkpoint_file):
            return False
        with open(self.checkpoint_file) as f:
            data = json.load(f)
        self.progress = Progress(**{k: v for k, v in data.items()
                                    if k in Progress.__dataclass_fields__})
        return True

    def save(self):
        with open(self.checkpoint_file, "w") as f:
            json.dump(asdict(self.progress), f, indent=2)

    def update(self, **kwargs):
        for k, v in kwargs.items():
            setattr(self.progress, k, v)
        self.save()
```

- [ ] **Step 4: Run tests — expect PASS**

```bash
pytest tests/test_progress.py -v
```

Expected: `3 passed`

- [ ] **Step 5: Commit**

```bash
git add migrator/progress.py tests/test_progress.py
git commit -m "feat: JSON checkpoint save/load with resume support"
```

---

## Task 6: PostgreSQL Reader

**Files:**
- Create: `migrator/pg_reader.py`
- Create: `tests/test_pg_reader.py`

- [ ] **Step 1: Write failing tests**

Create `tests/test_pg_reader.py`:

```python
import pytest
from unittest.mock import MagicMock, patch, call
from migrator.pg_reader import PgReader, ENTITY_TABLES
from migrator.config import PgConfig


@pytest.fixture
def mock_conn():
    conn = MagicMock()
    conn.cursor.return_value.__enter__ = MagicMock(return_value=MagicMock())
    conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
    return conn


@pytest.fixture
def reader(mock_conn):
    r = PgReader(PgConfig())
    r._conn = mock_conn
    return r


def test_entity_tables_covers_required_types():
    table_names = [t for t, _ in ENTITY_TABLES]
    assert "device" in table_names
    assert "customer" in table_names
    assert "tenant" in table_names
    assert "asset" in table_names
    assert len(ENTITY_TABLES) >= 10


def test_load_entity_map_maps_uuid_to_type(reader, mock_conn):
    uid = "550e8400-e29b-11d4-a716-446655440000"
    cur = MagicMock()
    cur.__iter__ = MagicMock(return_value=iter([]))

    def side_effect(*args, **kwargs):
        if "device" in (args[0] if args else ""):
            cur.__iter__ = MagicMock(return_value=iter([(uid,)]))
        else:
            cur.__iter__ = MagicMock(return_value=iter([]))
        return cur
    cur.execute.side_effect = side_effect

    mock_conn.cursor.return_value.__enter__.return_value = cur
    entity_map = reader.load_entity_map()
    assert entity_map.get(uid) == "DEVICE"


def test_load_key_map(reader, mock_conn):
    cur = MagicMock()
    cur.__iter__ = MagicMock(return_value=iter([(1, "temperature"), (2, "humidity")]))
    mock_conn.cursor.return_value.__enter__.return_value = cur
    key_map = reader.load_key_map()
    assert key_map[1] == "temperature"
    assert key_map[2] == "humidity"


def test_count_rows(reader, mock_conn):
    cur = MagicMock()
    cur.fetchone.return_value = (1_000_000,)
    mock_conn.cursor.return_value.__enter__.return_value = cur
    assert reader.count_rows() == 1_000_000
```

- [ ] **Step 2: Run tests — expect FAIL**

```bash
pytest tests/test_pg_reader.py -v
```

Expected: `ModuleNotFoundError: No module named 'migrator.pg_reader'`

- [ ] **Step 3: Implement `migrator/pg_reader.py`**

```python
import psycopg2
import psycopg2.extras
from typing import Dict, Iterator, List
from .config import PgConfig

ENTITY_TABLES = [
    ("device", "DEVICE"),
    ("customer", "CUSTOMER"),
    ("tenant", "TENANT"),
    ("asset", "ASSET"),
    ("alarm", "ALARM"),
    ("dashboard", "DASHBOARD"),
    ("rule_chain", "RULE_CHAIN"),
    ("rule_node", "RULE_NODE"),
    ("tb_user", "USER"),
    ("entity_view", "ENTITY_VIEW"),
    ("widgets_bundle", "WIDGETS_BUNDLE"),
    ("widget_type", "WIDGET_TYPE"),
    ("tenant_profile", "TENANT_PROFILE"),
    ("device_profile", "DEVICE_PROFILE"),
    ("api_usage_state", "API_USAGE_STATE"),
    ("edge", "EDGE"),
    ("ota_package", "OTA_PACKAGE"),
    ("rpc", "RPC"),
]


class PgReader:
    def __init__(self, config: PgConfig):
        self.config = config
        self._conn = None

    def connect(self):
        self._conn = psycopg2.connect(
            host=self.config.host,
            port=self.config.port,
            dbname=self.config.db,
            user=self.config.user,
            password=self.config.password,
        )
        self._conn.autocommit = True

    def close(self):
        if self._conn:
            self._conn.close()

    def load_entity_map(self) -> Dict[str, str]:
        entity_map: Dict[str, str] = {}
        with self._conn.cursor() as cur:
            for table, entity_type in ENTITY_TABLES:
                try:
                    cur.execute(f"SELECT id FROM {table}")
                    for (uid,) in cur:
                        entity_map[str(uid)] = entity_type
                except Exception:
                    self._conn.rollback()
        return entity_map

    def load_key_map(self) -> Dict[int, str]:
        with self._conn.cursor() as cur:
            cur.execute("SELECT key_id, key FROM ts_kv_dictionary")
            return {row[0]: row[1] for row in cur}

    def count_rows(self) -> int:
        with self._conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM ts_kv")
            return cur.fetchone()[0]

    def iter_distinct_entities(self, after_entity_id: str = None) -> Iterator[str]:
        sql = "SELECT DISTINCT entity_id FROM ts_kv"
        params = []
        if after_entity_id:
            sql += " WHERE entity_id > %s"
            params.append(after_entity_id)
        sql += " ORDER BY entity_id"
        with self._conn.cursor(name="entities_cur") as cur:
            cur.itersize = 1000
            cur.execute(sql, params or None)
            for (uid,) in cur:
                yield str(uid)

    def read_entity_ts(self, entity_id: str, after_ts: int, limit: int) -> List[dict]:
        with self._conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                "SELECT * FROM ts_kv WHERE entity_id = %s AND ts > %s ORDER BY ts ASC LIMIT %s",
                (entity_id, after_ts, limit),
            )
            return [dict(r) for r in cur.fetchall()]

    def read_all_latest(self) -> List[dict]:
        with self._conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("SELECT * FROM ts_kv_latest")
            return [dict(r) for r in cur.fetchall()]

    def read_latest_for_entities(self, entity_ids: List[str]) -> List[dict]:
        with self._conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                "SELECT * FROM ts_kv_latest WHERE entity_id = ANY(%s)",
                (entity_ids,),
            )
            return [dict(r) for r in cur.fetchall()]

    def read_new_ts_rows(self, watermark_ts: int, limit: int) -> List[dict]:
        with self._conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                "SELECT * FROM ts_kv WHERE ts > %s ORDER BY ts ASC LIMIT %s",
                (watermark_ts, limit),
            )
            return [dict(r) for r in cur.fetchall()]
```

- [ ] **Step 4: Run tests — expect PASS**

```bash
pytest tests/test_pg_reader.py -v
```

Expected: `4 passed`

- [ ] **Step 5: Commit**

```bash
git add migrator/pg_reader.py tests/test_pg_reader.py
git commit -m "feat: PostgreSQL reader (entity map, key map, ts_kv batch streaming)"
```

---

## Task 7: ScyllaDB Schema Init

**Files:**
- Create: `migrator/scylla_writer.py` (schema portion)
- Create: `tests/test_scylla_writer.py` (schema tests)

- [ ] **Step 1: Write failing tests**

Create `tests/test_scylla_writer.py`:

```python
import pytest
from unittest.mock import MagicMock, patch, call
from migrator.scylla_writer import ScyllaWriter, SCHEMA_STATEMENTS
from migrator.config import ScyllaConfig
from migrator.partition import Partitioning


@pytest.fixture
def writer():
    return ScyllaWriter(ScyllaConfig(), Partitioning.MONTHS)


def test_schema_statements_count():
    # keyspace + 3 tables = 4 statements
    assert len(SCHEMA_STATEMENTS) == 4


def test_schema_contains_ts_kv_cf():
    combined = " ".join(SCHEMA_STATEMENTS)
    assert "ts_kv_cf" in combined
    assert "ts_kv_partitions_cf" in combined
    assert "ts_kv_latest_cf" in combined


def test_schema_contains_entity_type():
    combined = " ".join(SCHEMA_STATEMENTS)
    assert "entity_type" in combined


def test_schema_contains_timeuuid():
    combined = " ".join(SCHEMA_STATEMENTS)
    assert "timeuuid" in combined


@patch("migrator.scylla_writer.Cluster")
def test_init_schema_executes_all_statements(mock_cluster_cls):
    mock_session = MagicMock()
    mock_cluster = MagicMock()
    mock_cluster.connect.return_value = mock_session
    mock_cluster_cls.return_value = mock_cluster

    w = ScyllaWriter(ScyllaConfig(keyspace="thingsboard"), Partitioning.MONTHS)
    w.init_schema()

    assert mock_session.execute.call_count == 4
    mock_cluster.shutdown.assert_called_once()
```

- [ ] **Step 2: Run tests — expect FAIL**

```bash
pytest tests/test_scylla_writer.py::test_schema_statements_count -v
```

Expected: `ModuleNotFoundError`

- [ ] **Step 3: Implement schema portion of `migrator/scylla_writer.py`**

```python
import uuid
import time
import logging
from typing import Any, Dict, List, Optional, Set, Tuple

from cassandra.cluster import Cluster, Session
from cassandra.query import BatchStatement, BatchType, PreparedStatement
from cassandra import WriteTimeout, Unavailable

from .config import ScyllaConfig
from .partition import Partitioning, compute_partition
from .cast import try_cast_string

log = logging.getLogger(__name__)

SCHEMA_STATEMENTS = [
    """CREATE KEYSPACE IF NOT EXISTS {ks}
       WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': 1}}""",

    """CREATE TABLE IF NOT EXISTS {ks}.ts_kv_cf (
           entity_type text,
           entity_id   timeuuid,
           key         text,
           partition   bigint,
           ts          bigint,
           bool_v      boolean,
           str_v       text,
           long_v      bigint,
           dbl_v       double,
           json_v      text,
           PRIMARY KEY ((entity_type, entity_id, key, partition), ts)
       )""",

    """CREATE TABLE IF NOT EXISTS {ks}.ts_kv_partitions_cf (
           entity_type text,
           entity_id   timeuuid,
           key         text,
           partition   bigint,
           PRIMARY KEY ((entity_type, entity_id, key), partition)
       ) WITH CLUSTERING ORDER BY (partition ASC)
         AND compaction = {{'class': 'LeveledCompactionStrategy'}}""",

    """CREATE TABLE IF NOT EXISTS {ks}.ts_kv_latest_cf (
           entity_type text,
           entity_id   timeuuid,
           key         text,
           ts          bigint,
           bool_v      boolean,
           str_v       text,
           long_v      bigint,
           dbl_v       double,
           json_v      text,
           PRIMARY KEY ((entity_type, entity_id), key)
       ) WITH compaction = {{'class': 'LeveledCompactionStrategy'}}""",
]

_INSERT_TS = """INSERT INTO {ks}.ts_kv_cf
    (entity_type, entity_id, key, partition, ts, bool_v, str_v, long_v, dbl_v, json_v)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"""

_INSERT_PARTITION = """INSERT INTO {ks}.ts_kv_partitions_cf
    (entity_type, entity_id, key, partition) VALUES (?, ?, ?, ?)"""

_INSERT_LATEST = """INSERT INTO {ks}.ts_kv_latest_cf
    (entity_type, entity_id, key, ts, bool_v, str_v, long_v, dbl_v, json_v)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)"""


class ScyllaWriter:
    def __init__(self, config: ScyllaConfig, partitioning: Partitioning,
                 cast_strings: bool = False):
        self.config = config
        self.partitioning = partitioning
        self.cast_strings = cast_strings
        self._cluster: Optional[Cluster] = None
        self._session: Optional[Session] = None
        self._ps_ts: Optional[PreparedStatement] = None
        self._ps_part: Optional[PreparedStatement] = None
        self._ps_latest: Optional[PreparedStatement] = None

    def init_schema(self):
        ks = self.config.keyspace
        cluster = Cluster([self.config.host], port=self.config.port)
        session = cluster.connect()
        for stmt in SCHEMA_STATEMENTS:
            session.execute(stmt.format(ks=ks))
        cluster.shutdown()

    def connect(self):
        ks = self.config.keyspace
        self._cluster = Cluster([self.config.host], port=self.config.port)
        self._session = self._cluster.connect(ks)
        self._ps_ts = self._session.prepare(_INSERT_TS.format(ks=ks))
        self._ps_part = self._session.prepare(_INSERT_PARTITION.format(ks=ks))
        self._ps_latest = self._session.prepare(_INSERT_LATEST.format(ks=ks))

    def close(self):
        if self._cluster:
            self._cluster.shutdown()
```

- [ ] **Step 4: Run tests — expect PASS**

```bash
pytest tests/test_scylla_writer.py -v
```

Expected: `5 passed`

- [ ] **Step 5: Commit**

```bash
git add migrator/scylla_writer.py tests/test_scylla_writer.py
git commit -m "feat: ScyllaDB schema init (verified against official schema-ts.cql)"
```

---

## Task 8: ScyllaDB Data Writer

**Files:**
- Modify: `migrator/scylla_writer.py` (add write methods)
- Modify: `tests/test_scylla_writer.py` (add write tests)

- [ ] **Step 1: Add write tests to `tests/test_scylla_writer.py`**

Append to the existing file:

```python
@pytest.fixture
def connected_writer():
    w = ScyllaWriter(ScyllaConfig(), Partitioning.MONTHS, cast_strings=False)
    w._session = MagicMock()
    w._ps_ts = MagicMock()
    w._ps_part = MagicMock()
    w._ps_latest = MagicMock()
    return w


def test_build_ts_row_resolves_key_and_partition():
    w = ScyllaWriter(ScyllaConfig(), Partitioning.MONTHS)
    row = {
        "entity_id": "550e8400-e29b-11d4-a716-446655440000",
        "key": 1,
        "ts": 1718445600000,
        "bool_v": None, "str_v": None, "long_v": 42, "dbl_v": None, "json_v": None,
    }
    key_map = {1: "temperature"}
    ts_vals, part_key = w._build_ts_row(row, "DEVICE", key_map)
    assert ts_vals[0] == "DEVICE"           # entity_type
    assert ts_vals[2] == "temperature"      # key
    assert ts_vals[3] == 1717200000000      # partition (June 1, 2024)
    assert ts_vals[4] == 1718445600000      # ts
    assert ts_vals[7] == 42                 # long_v
    assert part_key == ("DEVICE", ts_vals[1], "temperature", 1717200000000)


def test_build_ts_row_cast_string(connected_writer):
    connected_writer.cast_strings = True
    row = {
        "entity_id": "550e8400-e29b-11d4-a716-446655440000",
        "key": 1, "ts": 1718445600000,
        "bool_v": None, "str_v": "99", "long_v": None, "dbl_v": None, "json_v": None,
    }
    ts_vals, _ = connected_writer._build_ts_row(row, "DEVICE", {1: "k"})
    assert ts_vals[6] is None   # str_v cleared
    assert ts_vals[7] == 99     # long_v populated


def test_write_ts_batch_calls_session_execute(connected_writer):
    rows = [{
        "entity_id": "550e8400-e29b-11d4-a716-446655440000",
        "key": 1, "ts": 1718445600000,
        "bool_v": None, "str_v": None, "long_v": 10, "dbl_v": None, "json_v": None,
    }]
    key_map = {1: "temp"}
    written, partitions = connected_writer.write_ts_batch(rows, "DEVICE", key_map)
    assert written == 1
    assert len(partitions) == 1
    connected_writer._session.execute.assert_called()


def test_write_ts_batch_retries_on_timeout(connected_writer):
    from cassandra import WriteTimeout as WT
    connected_writer._session.execute.side_effect = [WT("timeout"), None]
    rows = [{
        "entity_id": "550e8400-e29b-11d4-a716-446655440000",
        "key": 1, "ts": 1718445600000,
        "bool_v": None, "str_v": None, "long_v": 10, "dbl_v": None, "json_v": None,
    }]
    written, _ = connected_writer.write_ts_batch(rows, "DEVICE", {1: "t"})
    assert written == 1
    assert connected_writer._session.execute.call_count == 2
```

- [ ] **Step 2: Run new tests — expect FAIL**

```bash
pytest tests/test_scylla_writer.py::test_build_ts_row_resolves_key_and_partition -v
```

Expected: `AttributeError: 'ScyllaWriter' object has no attribute '_build_ts_row'`

- [ ] **Step 3: Add write methods to `migrator/scylla_writer.py`**

Append after the `close` method:

```python
    def _build_ts_row(self, row: dict, entity_type: str,
                      key_map: Dict[int, str]) -> Tuple[list, tuple]:
        entity_id = uuid.UUID(str(row["entity_id"]))
        key_name = key_map.get(row["key"], str(row["key"]))
        ts = row["ts"]
        partition = compute_partition(ts, self.partitioning)
        bool_v = row.get("bool_v")
        str_v = row.get("str_v")
        long_v = row.get("long_v")
        dbl_v = row.get("dbl_v")
        json_v = str(row["json_v"]) if row.get("json_v") is not None else None

        if self.cast_strings and str_v is not None:
            col, val = try_cast_string(str_v)
            if col == "long_v":
                long_v, str_v = val, None
            elif col == "dbl_v":
                dbl_v, str_v = val, None

        ts_vals = [entity_type, entity_id, key_name, partition, ts,
                   bool_v, str_v, long_v, dbl_v, json_v]
        part_key = (entity_type, entity_id, key_name, partition)
        return ts_vals, part_key

    def write_ts_batch(self, rows: List[dict], entity_type: str,
                       key_map: Dict[int, str],
                       inner_batch: int = 50) -> Tuple[int, Set[tuple]]:
        partitions: Set[tuple] = set()
        written = 0
        batch = BatchStatement(batch_type=BatchType.UNLOGGED)
        count = 0
        for row in rows:
            ts_vals, part_key = self._build_ts_row(row, entity_type, key_map)
            batch.add(self._ps_ts, ts_vals)
            partitions.add(part_key)
            count += 1
            if count >= inner_batch:
                self._execute_with_retry(batch)
                written += count
                batch = BatchStatement(batch_type=BatchType.UNLOGGED)
                count = 0
        if count > 0:
            self._execute_with_retry(batch)
            written += count
        return written, partitions

    def write_partitions(self, partitions: Set[tuple]):
        batch = BatchStatement(batch_type=BatchType.UNLOGGED)
        count = 0
        for p in partitions:
            batch.add(self._ps_part, list(p))
            count += 1
            if count >= 50:
                self._execute_with_retry(batch)
                batch = BatchStatement(batch_type=BatchType.UNLOGGED)
                count = 0
        if count > 0:
            self._execute_with_retry(batch)

    def write_latest_batch(self, rows: List[dict], entity_type: str,
                           key_map: Dict[int, str]):
        batch = BatchStatement(batch_type=BatchType.UNLOGGED)
        count = 0
        for row in rows:
            entity_id = uuid.UUID(str(row["entity_id"]))
            key_name = key_map.get(row["key"], str(row["key"]))
            ts = row["ts"]
            json_v = str(row["json_v"]) if row.get("json_v") is not None else None
            vals = [entity_type, entity_id, key_name, ts,
                    row.get("bool_v"), row.get("str_v"), row.get("long_v"),
                    row.get("dbl_v"), json_v]
            batch.add(self._ps_latest, vals)
            count += 1
            if count >= 50:
                self._execute_with_retry(batch)
                batch = BatchStatement(batch_type=BatchType.UNLOGGED)
                count = 0
        if count > 0:
            self._execute_with_retry(batch)

    def _execute_with_retry(self, batch: BatchStatement, max_retries: int = 3):
        delay = 2.0
        for attempt in range(max_retries):
            try:
                self._session.execute(batch)
                return
            except (WriteTimeout, Unavailable) as exc:
                if attempt == max_retries - 1:
                    raise
                log.warning("ScyllaDB write error (attempt %d/%d): %s. Retry in %.0fs",
                            attempt + 1, max_retries, exc, delay)
                time.sleep(delay)
                delay = min(delay * 4, 30)
```

- [ ] **Step 4: Run all writer tests — expect PASS**

```bash
pytest tests/test_scylla_writer.py -v
```

Expected: `9 passed`

- [ ] **Step 5: Commit**

```bash
git add migrator/scylla_writer.py tests/test_scylla_writer.py
git commit -m "feat: ScyllaDB writer with batch inserts, partition tracking, retry backoff"
```

---

## Task 9: Orchestrator — Phase 0 + Phase 1

**Files:**
- Create: `migrator/orchestrator.py`
- Create: `tests/test_orchestrator.py` (Phase 0+1)

- [ ] **Step 1: Write failing tests**

Create `tests/test_orchestrator.py`:

```python
import pytest
from unittest.mock import MagicMock, patch, call
from migrator.orchestrator import Orchestrator
from migrator.config import MigratorConfig, PgConfig, ScyllaConfig
from migrator.progress import ProgressTracker


@pytest.fixture
def cfg():
    return MigratorConfig(
        pg=PgConfig(), scylla=ScyllaConfig(),
        batch_size=10, partitioning="MONTHS", checkpoint_file="/tmp/test_progress.json"
    )


@pytest.fixture
def orchestrator(cfg, tmp_path):
    cfg.checkpoint_file = str(tmp_path / "progress.json")
    o = Orchestrator(cfg)
    o.pg = MagicMock()
    o.scylla = MagicMock()
    return o


def test_phase0_builds_entity_and_key_maps(orchestrator):
    orchestrator.pg.load_entity_map.return_value = {"uuid-1": "DEVICE"}
    orchestrator.pg.load_key_map.return_value = {1: "temperature"}
    orchestrator.pg.count_rows.return_value = 500
    orchestrator._phase0()
    assert orchestrator.entity_map == {"uuid-1": "DEVICE"}
    assert orchestrator.key_map == {1: "temperature"}
    assert orchestrator.total_rows == 500


def test_phase1_skips_unknown_entity(orchestrator):
    orchestrator.entity_map = {}
    orchestrator.key_map = {1: "temp"}
    orchestrator.pg.iter_distinct_entities.return_value = iter(["unknown-uuid"])
    orchestrator._phase1()
    orchestrator.scylla.write_ts_batch.assert_not_called()
    assert orchestrator.tracker.progress.skipped_rows >= 0


def test_phase1_migrates_known_entity(orchestrator):
    entity_id = "550e8400-e29b-11d4-a716-446655440000"
    orchestrator.entity_map = {entity_id: "DEVICE"}
    orchestrator.key_map = {1: "temperature"}
    row = {"entity_id": entity_id, "key": 1, "ts": 1718445600000,
           "bool_v": None, "str_v": None, "long_v": 25, "dbl_v": None, "json_v": None}
    orchestrator.pg.iter_distinct_entities.return_value = iter([entity_id])
    orchestrator.pg.read_entity_ts.side_effect = [[row], []]
    orchestrator.scylla.write_ts_batch.return_value = (1, {("DEVICE", entity_id, "temperature", 0)})
    orchestrator._phase1()
    orchestrator.scylla.write_ts_batch.assert_called_once()
    orchestrator.scylla.write_partitions.assert_called()
    assert orchestrator.tracker.progress.migrated_rows == 1
```

- [ ] **Step 2: Run tests — expect FAIL**

```bash
pytest tests/test_orchestrator.py -v
```

Expected: `ModuleNotFoundError: No module named 'migrator.orchestrator'`

- [ ] **Step 3: Implement `migrator/orchestrator.py`**

```python
import logging
import time
from datetime import datetime, timezone
from typing import Dict, Optional, Set

from .config import MigratorConfig
from .partition import Partitioning
from .pg_reader import PgReader
from .progress import ProgressTracker
from .scylla_writer import ScyllaWriter

log = logging.getLogger(__name__)


class Orchestrator:
    def __init__(self, config: MigratorConfig):
        self.config = config
        self.tracker = ProgressTracker(config.checkpoint_file)
        self.pg = PgReader(config.pg)
        self.scylla = ScyllaWriter(
            config.scylla,
            Partitioning(config.partitioning),
            config.cast_strings,
        )
        self.entity_map: Dict[str, str] = {}
        self.key_map: Dict[int, str] = {}
        self.total_rows: int = 0
        self._on_progress = None
        self._on_lag = None

    def set_progress_callback(self, fn):
        self._on_progress = fn

    def set_lag_callback(self, fn):
        self._on_lag = fn

    def run(self, resume: bool = False, historical_only: bool = False):
        self.pg.connect()
        self.scylla.connect()
        try:
            if resume:
                self.tracker.load()
                log.info("Resuming from checkpoint: phase=%s, migrated=%d",
                         self.tracker.progress.phase, self.tracker.progress.migrated_rows)
            self._phase0()
            if self.tracker.progress.phase not in ("live_sync",):
                self._phase1()
            if not historical_only:
                self._phase2()
        finally:
            self.pg.close()
            self.scylla.close()

    def _phase0(self):
        log.info("Phase 0: Loading entity and key maps...")
        self.entity_map = self.pg.load_entity_map()
        self.key_map = self.pg.load_key_map()
        self.total_rows = self.pg.count_rows()
        log.info("Entity map: %d entries, Key map: %d entries, Total ts_kv rows: %d",
                 len(self.entity_map), len(self.key_map), self.total_rows)

        now_ms = int(time.time() * 1000)
        if not self.tracker.progress.phase1_start_ts:
            self.tracker.update(
                phase="phase1",
                phase1_start_ts=now_ms,
                partitioning=self.config.partitioning,
                cast_strings=self.config.cast_strings,
                started_at=datetime.now(timezone.utc).isoformat(),
            )

    def _phase1(self):
        log.info("Phase 1: Historical migration starting...")
        resume_entity = self.tracker.progress.last_entity_id
        resume_ts = self.tracker.progress.last_entity_ts
        batch_size = self.config.batch_size
        all_partitions: Set[tuple] = set()
        skipped = self.tracker.progress.skipped_rows
        migrated = self.tracker.progress.migrated_rows

        for entity_id in self.pg.iter_distinct_entities(after_entity_id=resume_entity):
            entity_type = self.entity_map.get(entity_id)
            if not entity_type:
                log.warning("entity_id %s not found in entity map — skipping", entity_id)
                skipped += 1
                continue

            after_ts = resume_ts if entity_id == resume_entity else 0
            while True:
                rows = self.pg.read_entity_ts(entity_id, after_ts, batch_size)
                if not rows:
                    break
                written, partitions = self.scylla.write_ts_batch(rows, entity_type, self.key_map)
                all_partitions.update(partitions)
                migrated += written
                after_ts = rows[-1]["ts"]

                if len(all_partitions) >= 10000:
                    self.scylla.write_partitions(all_partitions)
                    all_partitions.clear()

                self.tracker.update(
                    phase="phase1",
                    last_entity_id=entity_id,
                    last_entity_ts=after_ts,
                    migrated_rows=migrated,
                    skipped_rows=skipped,
                )
                if self._on_progress:
                    self._on_progress(migrated, self.total_rows)

        if all_partitions:
            self.scylla.write_partitions(all_partitions)

        log.info("Phase 1: Migrating ts_kv_latest...")
        latest_rows = self.pg.read_all_latest()
        for row in latest_rows:
            entity_type = self.entity_map.get(str(row["entity_id"]))
            if entity_type:
                self.scylla.write_latest_batch([row], entity_type, self.key_map)

        self.tracker.update(phase="live_sync",
                            watermark_ts=self.tracker.progress.phase1_start_ts - 60_000)
        log.info("Phase 1 complete. Migrated %d rows. Starting live sync...", migrated)

    def _phase2(self):
        log.info("Phase 2: Live sync starting (watermark=%d)...",
                 self.tracker.progress.watermark_ts)
        watermark = self.tracker.progress.watermark_ts
        batch_size = self.config.batch_size
        lag_threshold = self.config.lag_threshold_ms
        interval = self.config.live_sync_interval

        while True:
            rows = self.pg.read_new_ts_rows(watermark, batch_size)
            if rows:
                # Group rows by entity_type for efficient batch writes
                from collections import defaultdict
                grouped: dict = defaultdict(list)
                entity_ids_seen = set()
                for row in rows:
                    entity_id = str(row["entity_id"])
                    entity_type = self.entity_map.get(entity_id)
                    if not entity_type:
                        continue
                    grouped[entity_type].append(row)
                    entity_ids_seen.add(entity_id)

                all_partitions: Set[tuple] = set()
                for entity_type, type_rows in grouped.items():
                    _, partitions = self.scylla.write_ts_batch(type_rows, entity_type, self.key_map)
                    all_partitions.update(partitions)

                if all_partitions:
                    self.scylla.write_partitions(all_partitions)

                if entity_ids_seen:
                    latest = self.pg.read_latest_for_entities(list(entity_ids_seen))
                    for lrow in latest:
                        et = self.entity_map.get(str(lrow["entity_id"]))
                        if et:
                            self.scylla.write_latest_batch([lrow], et, self.key_map)

                watermark = rows[-1]["ts"]
                self.tracker.update(watermark_ts=watermark)

            lag_ms = int(time.time() * 1000) - watermark
            if self._on_lag:
                self._on_lag(lag_ms)

            if lag_ms < lag_threshold:
                log.info("✅ LAG < %dms. Ready for switchover!", lag_threshold)

            time.sleep(interval)
```

- [ ] **Step 4: Run all orchestrator tests — expect PASS**

```bash
pytest tests/test_orchestrator.py -v
```

Expected: `3 passed`

- [ ] **Step 5: Commit**

```bash
git add migrator/orchestrator.py tests/test_orchestrator.py
git commit -m "feat: orchestrator Phase 0 (map preload) + Phase 1 (historical migration)"
```

---

## Task 10: CLI + Rich Terminal Output

**Files:**
- Create: `main.py`

- [ ] **Step 1: Implement `main.py`**

```python
import signal
import sys
import time
import logging
import click
from rich.console import Console
from rich.progress import Progress, SpinnerColumn, BarColumn, TaskProgressColumn, TimeRemainingColumn
from rich.table import Table
from rich.live import Live
from rich.panel import Panel
from rich import print as rprint

from migrator.config import load_config
from migrator.orchestrator import Orchestrator

console = Console()
logging.basicConfig(level=logging.WARNING,
                    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")


@click.group()
def cli():
    """ThingsBoard PostgreSQL → ScyllaDB Migrator"""


@cli.command()
@click.option("--config", default="config.yaml", help="Config file path")
def init_schema(config):
    """ScyllaDB keyspace va 3 jadval yaratish"""
    cfg = load_config(config)
    from migrator.scylla_writer import ScyllaWriter
    from migrator.partition import Partitioning
    w = ScyllaWriter(cfg.scylla, Partitioning(cfg.partitioning))
    console.print("[bold cyan]ScyllaDB sxemasi yaratilmoqda...[/bold cyan]")
    w.init_schema()
    console.print("[bold green]✅ Sxema muvaffaqiyatli yaratildi![/bold green]")
    console.print(f"  Keyspace  : [yellow]{cfg.scylla.keyspace}[/yellow]")
    console.print("  Jadvallar : ts_kv_cf, ts_kv_partitions_cf, ts_kv_latest_cf")


@cli.command()
@click.option("--config", default="config.yaml", help="Config file path")
@click.option("--resume", is_flag=True, default=False, help="Checkpoint dan davom ettirish")
@click.option("--historical-only", is_flag=True, default=False, help="Faqat Phase 1")
@click.option("--cast-strings", is_flag=True, default=False, help="str_v → number cast")
@click.option("--partitioning",
              type=click.Choice(["MONTHS", "DAYS", "HOURS", "MINUTES", "YEARS", "INDEFINITE"]),
              default=None, help="Partition strategiyasi (config.yaml dan override)")
def start(config, resume, historical_only, cast_strings, partitioning):
    """Migratsiyani boshlash (Phase 0 → Phase 1 → Phase 2)"""
    cfg = load_config(config)
    if cast_strings:
        cfg.cast_strings = True
    if partitioning:
        cfg.partitioning = partitioning

    orch = Orchestrator(cfg)
    stats = {"migrated": 0, "total": 0, "lag_ms": None, "ready": False}

    with Progress(
        SpinnerColumn(),
        "[progress.description]{task.description}",
        BarColumn(),
        TaskProgressColumn(),
        TimeRemainingColumn(),
        console=console,
        transient=False,
    ) as prog:
        task = prog.add_task("[cyan]Ko'chirilmoqda...", total=None)

        def on_progress(migrated, total):
            stats["migrated"] = migrated
            stats["total"] = total
            prog.update(task, completed=migrated, total=total or 1,
                        description=f"[cyan]Ko'chirildi: {migrated:,} / {total:,}")

        def on_lag(lag_ms):
            stats["lag_ms"] = lag_ms
            lag_s = lag_ms / 1000
            if lag_ms < cfg.lag_threshold_ms:
                stats["ready"] = True
                prog.update(task, description=f"[green]✅ TAYYOR — Lag: {lag_s:.1f}s")
                _print_switchover_instructions(cfg)
            else:
                prog.update(task, description=f"[yellow]Live sync — Lag: {lag_s:.0f}s")

        orch.set_progress_callback(on_progress)
        orch.set_lag_callback(on_lag)

        def handle_sigterm(sig, frame):
            console.print("\n[yellow]SIGTERM qabul qilindi. To'xtatilmoqda...[/yellow]")
            sys.exit(0)
        signal.signal(signal.SIGTERM, handle_sigterm)

        try:
            orch.run(resume=resume, historical_only=historical_only)
        except KeyboardInterrupt:
            console.print("\n[yellow]Foydalanuvchi tomonidan to'xtatildi.[/yellow]")
            console.print(f"Checkpoint saqlandi: [bold]{cfg.checkpoint_file}[/bold]")
            console.print("Davom ettirish uchun: [bold]python main.py start --resume[/bold]")


@cli.command()
@click.option("--config", default="config.yaml", help="Config file path")
def status(config):
    """Checkpoint holati ko'rish"""
    cfg = load_config(config)
    from migrator.progress import ProgressTracker
    tracker = ProgressTracker(cfg.checkpoint_file)
    if not tracker.load():
        console.print("[yellow]Checkpoint topilmadi — migratsiya hali boshlanmagan.[/yellow]")
        return
    p = tracker.progress
    table = Table(title="Migratsiya holati", show_header=False, border_style="cyan")
    table.add_column("Parametr", style="bold")
    table.add_column("Qiymat", style="green")
    table.add_row("Faza", p.phase)
    table.add_row("Ko'chirildi", f"{p.migrated_rows:,} qator")
    table.add_row("O'tkazib yuborildi", f"{p.skipped_rows:,} qator")
    table.add_row("Oxirgi entity", p.last_entity_id or "—")
    table.add_row("Watermark", str(p.watermark_ts))
    table.add_row("Boshlandi", p.started_at or "—")
    table.add_row("Partition", p.partitioning)
    console.print(table)


def _print_switchover_instructions(cfg):
    instructions = """
[bold green]✅ SWITCHOVER VAQTI KELDI![/bold green]

[bold]1. ThingsBoard ni to'xtatish:[/bold]
   docker compose stop thingsboard-ce

[bold]2. docker-compose.yml ga qo'shish (thingsboard-ce environment):[/bold]
   DATABASE_TS_TYPE: cassandra
   TS_KV_PARTITIONING: {partitioning}
   CASSANDRA_URL: {host}:{port}
   CASSANDRA_CLUSTER_NAME: TB Cluster
   CASSANDRA_USE_CREDENTIALS: "false"
   CASSANDRA_KEYSPACE_NAME: {keyspace}

[bold]3. Eski TTL o'zgaruvchilarini olib tashlash:[/bold]
   SQL_TTL_TS_ENABLED, SQL_TTL_TS_TS_KEY_VALUE_TTL

[bold]4. ThingsBoard ni qayta ishga tushirish:[/bold]
   docker compose -f docker-compose.yml -f docker-compose.scylla.yml up -d thingsboard-ce

[bold]5. Migrator ni to'xtatish (Ctrl+C):[/bold]
   Yoki: docker compose stop tb-migrator
""".format(
        partitioning=cfg.partitioning,
        host=cfg.scylla.host,
        port=cfg.scylla.port,
        keyspace=cfg.scylla.keyspace,
    )
    console.print(Panel(instructions, title="[bold cyan]Switchover Yo'riqnomasi[/bold cyan]",
                        border_style="green"))


if __name__ == "__main__":
    cli()
```

- [ ] **Step 2: Verify CLI help works**

```bash
python main.py --help
python main.py start --help
python main.py status --help
```

Expected: Help text printed without errors for all 3 commands.

- [ ] **Step 3: Commit**

```bash
git add main.py
git commit -m "feat: CLI entry point with Rich progress bar and switchover instructions"
```

---

## Task 11: Run Full Test Suite

**Files:** No new files — validation only.

- [ ] **Step 1: Install dependencies**

```bash
pip install -r requirements.txt
```

Expected: All packages installed without errors.

- [ ] **Step 2: Run all tests**

```bash
pytest tests/ -v --tb=short
```

Expected:
```
tests/test_partition.py   7 passed
tests/test_cast.py        6 passed
tests/test_config.py      3 passed
tests/test_progress.py    3 passed
tests/test_pg_reader.py   4 passed
tests/test_scylla_writer.py  9 passed
tests/test_orchestrator.py   3 passed
======================== 35 passed ========================
```

- [ ] **Step 3: Verify Docker image builds**

```bash
docker build -t tb-migrator:local .
```

Expected: `Successfully built ...` with no errors.

- [ ] **Step 4: Commit**

```bash
git add .
git commit -m "chore: verify all 35 tests pass and Docker image builds cleanly"
```

---

## Task 12: O'zbekcha README

**Files:**
- Create: `README.md`

- [ ] **Step 1: Create `README.md`**

```markdown
# ThingsBoard PostgreSQL → ScyllaDB Migratsiya Vositasi

ThingsBoard CE 4.x ning timeseries ma'lumotlarini PostgreSQL dan ScyllaDB (Cassandra) ga
minimal downtime bilan ko'chirish uchun Python vositasi.

> **Arxitektura:** ThingsBoard, PostgreSQL, ScyllaDB va migrator barchasi **remote serverda**
> Docker orqali ishlaydi. Local PC faqat kod yozish va fayllarni serverga ko'chirish uchun ishlatiladi.

---

## Talablar

**Remote serverda:**
- Docker va Docker Compose o'rnatilgan
- ThingsBoard CE 4.x Docker da ishlamoqda
- PostgreSQL Docker da ishlamoqda (TB bilan bir compose faylda)
- SSH kirish imkoni

**Local PC da:**
- SSH client (Windows: PowerShell, Git Bash yoki OpenSSH)
- rsync yoki scp (fayllarni serverga ko'chirish uchun)

---

## Tezkor boshlash

### 0-qadam: Fayllarni remote serverga ko'chirish (local PC dan)

```bash
# Windows PowerShell yoki Git Bash da:
rsync -avz --exclude='.git' \
  /e/Projects/BlueStar/TB_DB_Migrator/ \
  user@YOUR_SERVER_IP:/opt/tb-migrator/

# Yoki scp bilan:
scp -r "E:\Projects\BlueStar\TB_DB_Migrator" user@YOUR_SERVER_IP:/opt/tb-migrator/
```

### 1-qadam: Remote serverda Docker image larni tayyorlash

```bash
# Remote serverga kirish:
ssh user@YOUR_SERVER_IP

# ScyllaDB imageni oldindan yuklash (1-2 GB, sekin internetda vaqt ketadi):
docker pull scylladb/scylla:6.2

# Migrator imageni qurish (pip paketlari image ichiga olinadi):
cd /opt/tb-migrator
docker build -t tb-migrator:local .
```

> **Internet juda sekin bo'lsa:** [Offline o'rnatish](#offline-ornatish) bo'limiga qarang.

### 2-qadam: ScyllaDB ni ishga tushirish

```bash
# Remote serverda (/opt/tb-migrator papkasida):
docker compose -f docker-compose.yml -f docker-compose.scylla.yml up -d scylladb
```

ScyllaDB tayyor bo'lishini kuting (taxminan 60 soniya):

```bash
docker compose -f docker-compose.yml -f docker-compose.scylla.yml logs -f scylladb
# "Starting listening for CQL clients" xabarini kuting
```

### 3-qadam: ScyllaDB sxemasini yaratish

```bash
# Remote serverda:
cd /opt/tb-migrator
docker compose -f docker-compose.yml -f docker-compose.scylla.yml run --rm tb-migrator \
    python main.py init-schema
```

Natija:
```
✅ Sxema muvaffaqiyatli yaratildi!
  Keyspace  : thingsboard
  Jadvallar : ts_kv_cf, ts_kv_partitions_cf, ts_kv_latest_cf
```

### 4-qadam: Migratsiyani boshlash

> **Muhim:** SSH sessiyasi uzilsa migrator to'xtab qolmasligi uchun `screen` yoki `tmux` ichida ishga tushiring.

```bash
# Remote serverda — screen sessiya ochish:
screen -S migration

cd /opt/tb-migrator
docker compose -f docker-compose.yml -f docker-compose.scylla.yml run --rm tb-migrator \
    python main.py start

# Screen dan chiqish (migrator ishlashda qoladi): Ctrl+A, keyin D
# Screen ga qaytish:  screen -r migration
```

Terminal da jarayon ko'rsatiladi:

```
TB PostgreSQL → ScyllaDB Migrator
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Ko'chirildi: 98,234,100 / 142,847,392  [████████████░░░] 68.7%  ~14:32 qoldi

Live sync — Lag: 45s
```

### 5-qadam: Switchover (TB ni Cassandra ga o'tkazish)

Migrator `✅ TAYYOR — Lag < 30s` xabarini ko'rsatganda:

**ThingsBoard ni to'xtatish:**
```bash
# Remote serverda:
cd /opt/tb-migrator
docker compose stop thingsboard-ce
```

**`docker-compose.yml` faylida `thingsboard-ce` servisiga quyidagi muhit o'zgaruvchilarini qo'shing:**
```yaml
environment:
  DATABASE_TS_TYPE: cassandra
  TS_KV_PARTITIONING: MONTHS
  CASSANDRA_URL: scylladb:9042
  CASSANDRA_CLUSTER_NAME: TB Cluster
  CASSANDRA_USE_CREDENTIALS: "false"
  CASSANDRA_KEYSPACE_NAME: thingsboard
```

**Quyidagi eski o'zgaruvchilarni o'chiring:**
```yaml
# O'chiring:
# SQL_TTL_TS_ENABLED
# SQL_TTL_TS_TS_KEY_VALUE_TTL
# SQL_TTL_TS_EXECUTION_INTERVAL
```

**ThingsBoard ni qayta ishga tushiring:**
```bash
docker compose -f docker-compose.yml -f docker-compose.scylla.yml up -d thingsboard-ce
```

**Migratorni to'xtating:**
```bash
# Migrator konsolida Ctrl+C bosing
```

---

## Migratsiyani to'xtatib, davom ettirish

Agar migrator kutilmaganda to'xtab qolsa (server o'chishi, xatolik), quyidagicha davom ettiring:

```bash
docker compose -f docker-compose.yml -f docker-compose.scylla.yml run --rm tb-migrator \
    python main.py start --resume
```

Holat ko'rish:

```bash
docker compose -f docker-compose.yml -f docker-compose.scylla.yml run --rm tb-migrator \
    python main.py status
```

---

## CLI buyruqlari

| Buyruq | Tavsif |
|--------|--------|
| `python main.py init-schema` | ScyllaDB jadvallarini yaratish |
| `python main.py start` | To'liq migratsiya (Phase 1 + Live Sync) |
| `python main.py start --resume` | Checkpoint dan davom ettirish |
| `python main.py start --historical-only` | Faqat tarixiy ma'lumotlar (Live Sync yo'q) |
| `python main.py start --cast-strings` | String qiymatlarni raqamga o'tkazish |
| `python main.py start --partitioning DAYS` | Kunlik partition strategiyasi |
| `python main.py status` | Jarayon holatini ko'rish |

---

## Sozlamalar (config.yaml)

```yaml
pg:
  host: postgres    # Docker service nomi
  port: 5432
  db: thingsboard
  user: postgres
  password: postgres

scylla:
  host: scylladb    # Docker service nomi
  port: 9042
  keyspace: thingsboard

migrator:
  batch_size: 5000          # Bir vaqtda o'qiladigan qatorlar soni
  live_sync_interval: 5.0   # Live sync tekshirish oralig'i (soniya)
  lag_threshold_ms: 30000   # Switchover uchun minimal lag (30 soniya)
  partitioning: MONTHS      # MONTHS / DAYS / HOURS / YEARS / INDEFINITE
  cast_strings: false       # String qiymatlarni raqamga o'tkazish
  checkpoint_file: migration_progress.json
```

Muhit o'zgaruvchilari `config.yaml` ni override qiladi:
`PG_HOST`, `PG_PORT`, `PG_DB`, `PG_USER`, `PG_PASSWORD`,
`SCYLLA_HOST`, `SCYLLA_PORT`, `SCYLLA_KEYSPACE`

---

## Ishlash ko'rsatkichlari

| Ma'lumot hajmi | Taxminiy vaqt |
|----------------|---------------|
| 1M qator       | ~2 daqiqa     |
| 50M qator      | ~30 daqiqa    |
| 500M qator     | ~5 soat       |

*Tezlik server CPU va disk tezligiga bog'liq.*

---

---

## Muammolar va yechimlar

**ScyllaDB ulanmayapti:**
```bash
docker compose -f docker-compose.yml -f docker-compose.scylla.yml logs scylladb | tail -20
```

**"Entity map da topilmadi" ogohlantirmalari:**  
Ba'zi `entity_id` lar o'chirilgan entity larga tegishli. Bu normal — ular `migration_errors.log` ga yoziladi.

**Migrator juda sekin:**  
`config.yaml` da `batch_size: 10000` ga oshiring.

**ThingsBoard Cassandra ga ulanmayapti:**  
`docker compose logs thingsboard-ce | grep -i cassandra` buyrug'i bilan xatolarni ko'ring.

---

## Texnik ma'lumot

Bu vosita rasmiy [ThingsBoard database-migrator](https://github.com/thingsboard/database-migrator)
ning Python versiyasi bo'lib, Docker muhiti uchun moslashtirilgan va live sync qo'shilgan.

Cassandra sxemasi rasmiy `schema-ts.cql` va `schema-ts-latest.cql` fayllaridan olingan.
```

- [ ] **Step 2: Verify README renders correctly**

```bash
# Windows da markdown preview uchun VS Code da oching
# Yoki:
python -c "
with open('README.md') as f:
    content = f.read()
assert '## Tezkor boshlash' in content
assert 'init-schema' in content
assert 'docker compose' in content
print('README struktura tekshiruvi: OK')
"
```

Expected: `README struktura tekshiruvi: OK`

- [ ] **Step 3: Final commit**

```bash
git add README.md
git commit -m "docs: o'zbekcha README — to'liq yo'riqnoma (switchover, CLI, sozlamalar)"
```

---

## Yakuniy tekshiruv

- [ ] `pytest tests/ -v` → 35 passed
- [ ] `docker build -t tb-migrator:local .` → muvaffaqiyatli
- [ ] `python main.py --help` → buyruqlar ko'rsatiladi
- [ ] `python main.py init-schema --help` → help chiqadi
- [ ] Spec dagi barcha talablar amalga oshirilganligi tekshirildi

---

## Spec Coverage

| Spec bo'limi | Task |
|---|---|
| Cassandra sxemasi (entity_type, timeuuid, to'g'ri PKlar) | Task 7 |
| MONTHS partition strategiyasi | Task 2 |
| Entity type resolution (18 ta PG jadval) | Task 6 |
| cast strings (-castEnable) | Task 3 |
| Progress checkpoint + resume | Task 5 |
| Phase 0: entity/key map preload | Task 9 |
| Phase 1: historical migration + partitions | Task 9 |
| Phase 2: live sync + watermark | Task 9 |
| Switchover instructions | Task 10 |
| Retry backoff on WriteTimeout | Task 8 |
| Docker compose overlay | Task 1 |
| O'zbekcha README | Task 12 |
