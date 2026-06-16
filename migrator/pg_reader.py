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
        """Returns {key_id: key_name} from ts_kv_dictionary."""
        with self._conn.cursor() as cur:
            cur.execute("SELECT key_id, key FROM ts_kv_dictionary")
            return {row[0]: row[1] for row in cur.fetchall()}

    def count_ts_kv(self) -> int:
        """Returns total row count of ts_kv table."""
        with self._conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM ts_kv")
            return cur.fetchone()[0]

    def iter_distinct_entities(self) -> Generator[str, None, None]:
        """Yields distinct entity_id strings using a server-side cursor."""
        with self._conn.cursor("distinct_entities") as cur:
            cur.itersize = 1000
            cur.execute("SELECT DISTINCT entity_id FROM ts_kv")
            for (entity_id,) in cur:
                yield str(entity_id)

    def iter_ts_kv_for_entity(
        self,
        entity_id: str,
        batch_size: int = 5000,
        min_ts: Optional[int] = None,
    ) -> Generator[dict, None, None]:
        """Yields rows from ts_kv for the given entity_id, in batches."""
        query = "SELECT entity_id, key, ts, bool_v, str_v, long_v, dbl_v, json_v FROM ts_kv WHERE entity_id = %s"
        params: list = [entity_id]
        if min_ts is not None:
            query += " AND ts > %s"
            params.append(min_ts)
        query += " ORDER BY ts ASC"

        offset = 0
        while True:
            paged = query + f" LIMIT {batch_size} OFFSET {offset}"
            with self._conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(paged, params)
                rows = cur.fetchall()
            if not rows:
                break
            for row in rows:
                yield dict(row)
            if len(rows) < batch_size:
                break
            offset += batch_size

    def iter_ts_kv_by_ts(
        self,
        watermark_ts: int,
        batch_size: int = 5000,
    ) -> Generator[dict, None, None]:
        """Yields ts_kv rows with ts > watermark_ts, ordered by ts ASC. Used for live sync."""
        offset = 0
        while True:
            query = (
                "SELECT entity_id, key, ts, bool_v, str_v, long_v, dbl_v, json_v "
                "FROM ts_kv WHERE ts > %s ORDER BY ts ASC LIMIT %s OFFSET %s"
            )
            with self._conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(query, [watermark_ts, batch_size, offset])
                rows = cur.fetchall()
            if not rows:
                break
            for row in rows:
                yield dict(row)
            if len(rows) < batch_size:
                break
            offset += batch_size

    def iter_ts_kv_latest(self) -> Generator[dict, None, None]:
        """Yields all rows from ts_kv_latest."""
        with self._conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                "SELECT entity_id, key, ts, bool_v, str_v, long_v, dbl_v, json_v FROM ts_kv_latest"
            )
            for row in cur:
                yield dict(row)
