import logging
import time
import uuid
from typing import Dict, List, Optional, Set, Tuple

from cassandra import WriteTimeout
from cassandra.cluster import Cluster, Session
from cassandra.policies import DCAwareRoundRobinPolicy
from cassandra.query import BatchStatement, BatchType, PreparedStatement

logger = logging.getLogger(__name__)

SCHEMA_STATEMENTS = [
    """CREATE KEYSPACE IF NOT EXISTS {keyspace}
       WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': 1}}""",

    """CREATE TABLE IF NOT EXISTS {keyspace}.ts_kv_cf (
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

    """CREATE TABLE IF NOT EXISTS {keyspace}.ts_kv_partitions_cf (
           entity_type text,
           entity_id   timeuuid,
           key         text,
           partition   bigint,
           PRIMARY KEY ((entity_type, entity_id, key), partition)
       ) WITH CLUSTERING ORDER BY (partition ASC)
         AND compaction = {{'class': 'LeveledCompactionStrategy'}}""",

    """CREATE TABLE IF NOT EXISTS {keyspace}.ts_kv_latest_cf (
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


class ScyllaWriter:
    def __init__(self, session: Session, keyspace: str):
        self._session = session
        self._keyspace = keyspace
        self._ps_ts: Optional[PreparedStatement] = None
        self._ps_partition: Optional[PreparedStatement] = None
        self._ps_latest: Optional[PreparedStatement] = None

    @classmethod
    def connect(cls, host: str, port: int, keyspace: str) -> "ScyllaWriter":
        cluster = Cluster(
            [host],
            port=port,
            load_balancing_policy=DCAwareRoundRobinPolicy(),
        )
        session = cluster.connect()
        return cls(session, keyspace)

    def init_schema(self):
        """Create keyspace and tables if they don't exist."""
        for stmt in SCHEMA_STATEMENTS:
            self._session.execute(stmt.format(keyspace=self._keyspace))

    def _prepare_statements(self):
        ks = self._keyspace
        if self._ps_ts is None:
            self._ps_ts = self._session.prepare(
                f"INSERT INTO {ks}.ts_kv_cf "
                "(entity_type, entity_id, key, partition, ts, bool_v, str_v, long_v, dbl_v, json_v) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
            )
        if self._ps_partition is None:
            self._ps_partition = self._session.prepare(
                f"INSERT INTO {ks}.ts_kv_partitions_cf "
                "(entity_type, entity_id, key, partition) VALUES (?, ?, ?, ?)"
            )
        if self._ps_latest is None:
            self._ps_latest = self._session.prepare(
                f"INSERT INTO {ks}.ts_kv_latest_cf "
                "(entity_type, entity_id, key, ts, bool_v, str_v, long_v, dbl_v, json_v) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)"
            )

    def _build_ts_row(self, row: dict, entity_type: str, key_name: str, partition: int) -> tuple:
        """Build a tuple for ts_kv_cf INSERT from a pg row dict."""
        eid = uuid.UUID(str(row["entity_id"]))
        return (
            entity_type,
            eid,
            key_name,
            partition,
            row["ts"],
            row.get("bool_v"),
            row.get("str_v"),
            row.get("long_v"),
            row.get("dbl_v"),
            row.get("json_v"),
        )

    def write_ts_batch(
        self,
        rows: List[dict],
        entity_type: str,
        key_map: Dict[int, str],
        partition_fn,
        cast_fn=None,
        max_retries: int = 3,
    ) -> Set[Tuple]:
        """Write a batch of ts_kv rows. Returns set of (entity_type, entity_id_str, key, partition) tuples for partition table."""
        self._prepare_statements()
        partitions_seen: Set[Tuple] = set()
        batch_size = len(rows)
        attempt = 0

        while attempt < max_retries:
            try:
                batch = BatchStatement(batch_type=BatchType.UNLOGGED)
                for row in rows:
                    key_id = row["key"]
                    key_name = key_map.get(key_id, str(key_id))
                    ts = row["ts"]
                    partition = partition_fn(ts)

                    # Apply cast if provided (modifies str_v → long_v or dbl_v)
                    effective_row = dict(row)
                    if cast_fn and effective_row.get("str_v") is not None:
                        col, val = cast_fn(effective_row["str_v"])
                        if col != "str_v":
                            effective_row["str_v"] = None
                            effective_row[col] = val

                    tup = self._build_ts_row(effective_row, entity_type, key_name, partition)
                    batch.add(self._ps_ts, tup)

                    eid_str = str(row["entity_id"])
                    partitions_seen.add((entity_type, eid_str, key_name, partition))

                self._session.execute(batch)
                return partitions_seen

            except WriteTimeout:
                attempt += 1
                if attempt >= max_retries:
                    raise
                # Halve batch — retry only first half
                rows = rows[: max(1, len(rows) // 2)]
                batch_size = len(rows)
                logger.warning("WriteTimeout: retrying with %d rows (attempt %d)", batch_size, attempt)
                time.sleep(2 ** attempt)

        return partitions_seen

    def write_partitions(self, partitions: Set[Tuple]):
        """Write partition index entries to ts_kv_partitions_cf."""
        self._prepare_statements()
        if not partitions:
            return
        batch = BatchStatement(batch_type=BatchType.UNLOGGED)
        for entity_type, eid_str, key_name, partition in partitions:
            eid = uuid.UUID(eid_str)
            batch.add(self._ps_partition, (entity_type, eid, key_name, partition))
        self._session.execute(batch)

    def write_latest_batch(
        self,
        rows: List[dict],
        entity_type: str,
        key_map: Dict[int, str],
        cast_fn=None,
    ):
        """Write a batch of ts_kv_latest rows to ts_kv_latest_cf."""
        self._prepare_statements()
        if not rows:
            return
        batch = BatchStatement(batch_type=BatchType.UNLOGGED)
        for row in rows:
            key_id = row["key"]
            key_name = key_map.get(key_id, str(key_id))
            eid = uuid.UUID(str(row["entity_id"]))

            effective_row = dict(row)
            if cast_fn and effective_row.get("str_v") is not None:
                col, val = cast_fn(effective_row["str_v"])
                if col != "str_v":
                    effective_row["str_v"] = None
                    effective_row[col] = val

            batch.add(self._ps_latest, (
                entity_type,
                eid,
                key_name,
                effective_row["ts"],
                effective_row.get("bool_v"),
                effective_row.get("str_v"),
                effective_row.get("long_v"),
                effective_row.get("dbl_v"),
                effective_row.get("json_v"),
            ))
        self._session.execute(batch)

    def _execute_with_retry(self, statement, params, max_retries: int = 3):
        """Execute a single CQL statement with exponential backoff on WriteTimeout."""
        for attempt in range(max_retries):
            try:
                self._session.execute(statement, params)
                return
            except WriteTimeout:
                if attempt == max_retries - 1:
                    raise
                wait = 2 ** (attempt + 1)
                logger.warning("WriteTimeout on single execute, retrying in %ds", wait)
                time.sleep(wait)
