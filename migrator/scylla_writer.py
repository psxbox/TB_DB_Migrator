import logging
import time
import uuid
from typing import Dict, List, Optional, Set, Tuple

from cassandra.cluster import Cluster, Session
from cassandra.concurrent import execute_concurrent_with_args
from cassandra.policies import DCAwareRoundRobinPolicy
from cassandra.query import PreparedStatement

logger = logging.getLogger(__name__)

# Concurrency for execute_concurrent. ScyllaDB handles high concurrency well;
# keep modest so a small (e.g. --smp 2) node isn't overwhelmed.
CONCURRENCY = 32

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
            protocol_version=4,
            compression=False,
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

    def _apply_cast(self, row: dict, cast_fn) -> dict:
        """Return a copy of row with str_v cast to long_v/dbl_v if cast_fn maps it."""
        effective_row = dict(row)
        if cast_fn and effective_row.get("str_v") is not None:
            col, val = cast_fn(effective_row["str_v"])
            if col != "str_v":
                effective_row["str_v"] = None
                effective_row[col] = val
        return effective_row

    def _execute_concurrent(self, prepared: PreparedStatement, params_list: List[tuple], max_retries: int = 4):
        """Execute many single-partition INSERTs concurrently, retrying only failed rows.

        Unlike a multi-partition BatchStatement, this never silently drops rows: failed
        statements are collected and retried with exponential backoff, and the first
        error is raised if retries are exhausted.
        """
        if not params_list:
            return
        pending = list(params_list)
        attempt = 0
        while True:
            results = execute_concurrent_with_args(
                self._session,
                prepared,
                pending,
                concurrency=CONCURRENCY,
                raise_on_first_error=False,
            )
            failed = [pending[i] for i, (ok, _) in enumerate(results) if not ok]
            if not failed:
                return
            attempt += 1
            if attempt >= max_retries:
                first_exc = next(res for ok, res in results if not ok)
                logger.error("%d rows still failing after %d attempts", len(failed), attempt)
                raise first_exc
            logger.warning(
                "%d/%d concurrent writes failed, retrying (attempt %d)",
                len(failed), len(pending), attempt,
            )
            time.sleep(2 ** attempt)
            pending = failed

    def write_ts_batch(
        self,
        rows: List[dict],
        entity_type: str,
        key_map: Dict[int, str],
        partition_fn,
        cast_fn=None,
    ) -> Set[Tuple]:
        """Write ts_kv rows. Returns set of (entity_type, entity_id_str, key, partition) tuples."""
        self._prepare_statements()
        params_list: List[tuple] = []
        partitions_seen: Set[Tuple] = set()

        for row in rows:
            key_id = row["key"]
            key_name = key_map.get(key_id, str(key_id))
            partition = partition_fn(row["ts"])
            effective_row = self._apply_cast(row, cast_fn)
            params_list.append(self._build_ts_row(effective_row, entity_type, key_name, partition))
            partitions_seen.add((entity_type, str(row["entity_id"]), key_name, partition))

        self._execute_concurrent(self._ps_ts, params_list)
        return partitions_seen

    def write_partitions(self, partitions: Set[Tuple]):
        """Write partition index entries to ts_kv_partitions_cf."""
        self._prepare_statements()
        if not partitions:
            return
        params = [
            (entity_type, uuid.UUID(eid_str), key_name, partition)
            for entity_type, eid_str, key_name, partition in partitions
        ]
        self._execute_concurrent(self._ps_partition, params)

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
        params_list: List[tuple] = []
        for row in rows:
            key_id = row["key"]
            key_name = key_map.get(key_id, str(key_id))
            eid = uuid.UUID(str(row["entity_id"]))
            effective_row = self._apply_cast(row, cast_fn)
            params_list.append((
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
        self._execute_concurrent(self._ps_latest, params_list)
