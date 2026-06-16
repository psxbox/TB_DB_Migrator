import logging
import psycopg2
import psycopg2.extras
from typing import Dict, Generator, List, Optional, Tuple

logger = logging.getLogger(__name__)

ENTITY_TABLES: Dict[str, str] = {
    "device": "DEVICE",
    "customer": "CUSTOMER",
    "tenant": "TENANT",
    "asset": "ASSET",
    "alarm": "ALARM",
    "dashboard": "DASHBOARD",
    "rule_chain": "RULE_CHAIN",
    "rule_node": "RULE_NODE",
    "tb_user": "USER",
    "entity_view": "ENTITY_VIEW",
    "widgets_bundle": "WIDGETS_BUNDLE",
    "widget_type": "WIDGET_TYPE",
    "tenant_profile": "TENANT_PROFILE",
    "device_profile": "DEVICE_PROFILE",
    "api_usage_state": "API_USAGE_STATE",
    "edge": "EDGE",
    "ota_package": "OTA_PACKAGE",
    "rpc": "RPC",
}


class PgReader:
    def __init__(self, conn):
        self._conn = conn

    def load_entity_map(self) -> Dict[str, str]:
        """Returns {uuid_str: entity_type_str} for all entity tables."""
        entity_map: Dict[str, str] = {}
        with self._conn.cursor() as cur:
            for table, entity_type in ENTITY_TABLES.items():
                try:
                    cur.execute(f"SELECT id FROM {table}")
                    for (row_id,) in cur.fetchall():
                        entity_map[str(row_id)] = entity_type
                except psycopg2.Error as e:
                    logger.warning("Failed to read table %s: %s", table, e)
                    self._conn.rollback()
        return entity_map

    def load_key_map(self) -> Dict[int, str]:
        """Returns {key_id: key_name} from key_dictionary (TB 4.x) or ts_kv_dictionary (older TB), or {} for pure-SQL mode."""
        for table in ("key_dictionary", "ts_kv_dictionary"):
            with self._conn.cursor() as cur:
                try:
                    cur.execute(f"SELECT key_id, key FROM {table}")
                    result = {row[0]: row[1] for row in cur.fetchall()}
                    logger.info("Loaded %d keys from %s", len(result), table)
                    return result
                except psycopg2.Error:
                    self._conn.rollback()
        logger.info("No key dictionary table found — pure-SQL mode, using ts_kv.key directly")
        return {}

    def count_ts_kv(self) -> int:
        """Returns total row count of ts_kv table."""
        with self._conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM ts_kv")
            return cur.fetchone()[0]

    def _has_rows(self, table: str) -> bool:
        with self._conn.cursor() as cur:
            cur.execute(f"SELECT 1 FROM {table} LIMIT 1")
            return cur.fetchone() is not None

    def iter_distinct_entities(self) -> Generator[str, None, None]:
        """Yields distinct entity_id strings using a server-side cursor.

        Reads from ts_kv_latest (one row per entity+key) which is orders of
        magnitude smaller than ts_kv, so the DISTINCT scan returns the first
        entity almost immediately instead of scanning all of ts_kv first.
        Falls back to ts_kv if ts_kv_latest is empty.
        """
        source = "ts_kv_latest" if self._has_rows("ts_kv_latest") else "ts_kv"
        logger.info("Reading distinct entities from %s", source)
        with self._conn.cursor("distinct_entities") as cur:
            cur.itersize = 1000
            cur.execute(f"SELECT DISTINCT entity_id FROM {source}")
            for (entity_id,) in cur:
                yield str(entity_id)

    def iter_ts_kv_for_entity(
        self,
        entity_id: str,
        batch_size: int = 5000,
        min_ts: Optional[int] = None,
    ) -> Generator[dict, None, None]:
        """Yields rows from ts_kv for the given entity_id, in batches.

        Uses keyset pagination on the (key, ts) primary-key prefix instead of
        LIMIT/OFFSET, so each page is an index seek (O(n) overall) rather than an
        ever-growing scan (O(n^2)).
        """
        cols = "entity_id, key, ts, bool_v, str_v, long_v, dbl_v, json_v"
        last_key = None
        last_ts = None
        while True:
            params: list = [entity_id]
            query = f"SELECT {cols} FROM ts_kv WHERE entity_id = %s"
            if last_key is not None:
                query += " AND (key, ts) > (%s, %s)"
                params.extend([last_key, last_ts])
            if min_ts is not None:
                query += " AND ts > %s"
                params.append(min_ts)
            query += " ORDER BY key ASC, ts ASC LIMIT %s"
            params.append(batch_size)

            with self._conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(query, params)
                rows = cur.fetchall()
            if not rows:
                break
            for row in rows:
                yield dict(row)
            if len(rows) < batch_size:
                break
            last_key = rows[-1]["key"]
            last_ts = rows[-1]["ts"]

    def iter_ts_kv_by_ts(
        self,
        watermark_ts: int,
        batch_size: int = 5000,
    ) -> Generator[dict, None, None]:
        """Yields ts_kv rows with ts > watermark_ts, ordered by (ts, entity_id, key). Used for live sync.

        Keyset pagination on (ts, entity_id, key) avoids OFFSET re-scans and never
        skips rows that share the same ts at a page boundary.
        """
        cols = "entity_id, key, ts, bool_v, str_v, long_v, dbl_v, json_v"
        last = None  # (ts, entity_id, key)
        while True:
            if last is None:
                query = f"SELECT {cols} FROM ts_kv WHERE ts > %s"
                params: list = [watermark_ts]
            else:
                query = f"SELECT {cols} FROM ts_kv WHERE (ts, entity_id, key) > (%s, %s, %s)"
                params = [last[0], last[1], last[2]]
            query += " ORDER BY ts ASC, entity_id ASC, key ASC LIMIT %s"
            params.append(batch_size)

            with self._conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(query, params)
                rows = cur.fetchall()
            if not rows:
                break
            for row in rows:
                yield dict(row)
            if len(rows) < batch_size:
                break
            r = rows[-1]
            last = (r["ts"], r["entity_id"], r["key"])

    def iter_ts_kv_latest(self) -> Generator[dict, None, None]:
        """Yields all rows from ts_kv_latest."""
        with self._conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                "SELECT entity_id, key, ts, bool_v, str_v, long_v, dbl_v, json_v FROM ts_kv_latest"
            )
            for row in cur:
                yield dict(row)
