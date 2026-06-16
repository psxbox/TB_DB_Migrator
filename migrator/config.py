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
    workers: int = 4
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
        workers=mg_d.get("workers", 4),
        live_sync_interval=mg_d.get("live_sync_interval", 5.0),
        lag_threshold_ms=mg_d.get("lag_threshold_ms", 30000),
        partitioning=mg_d.get("partitioning", "MONTHS"),
        cast_strings=mg_d.get("cast_strings", False),
        checkpoint_file=mg_d.get("checkpoint_file", "migration_progress.json"),
    )
