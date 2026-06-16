import logging
import signal
import threading
import time
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from typing import Callable, Optional

import psycopg2
import psycopg2.extras

from migrator.cast import try_cast_string
from migrator.config import MigratorConfig
from migrator.partition import Partitioning, compute_partition
from migrator.pg_reader import PgReader
from migrator.progress import ProgressTracker
from migrator.scylla_writer import ScyllaWriter

logger = logging.getLogger(__name__)


class Orchestrator:
    def __init__(
        self,
        config: MigratorConfig,
        reader: PgReader,
        writer: ScyllaWriter,
        tracker: ProgressTracker,
        conn_factory: Optional[Callable] = None,
    ):
        self._cfg = config
        self._reader = reader
        self._writer = writer
        self._tracker = tracker
        self._conn_factory = conn_factory
        self._stop = False
        self._partitioning = Partitioning(config.partitioning)
        self._partition_fn = lambda ts: compute_partition(ts, self._partitioning)
        self._cast_fn = try_cast_string if config.cast_strings else None

    def run(self, historical_only: bool = False, resume: bool = False):
        """Entry point: Phase 0 → Phase 1 → Phase 2 (unless historical_only)."""
        signal.signal(signal.SIGTERM, self._handle_sigterm)
        signal.signal(signal.SIGINT, self._handle_sigterm)

        if resume and self._tracker.load():
            logger.info("Resuming from checkpoint: phase=%s", self._tracker.progress.phase)
        else:
            self._tracker.update(
                phase="phase0",
                partitioning=self._cfg.partitioning,
                cast_strings=self._cfg.cast_strings,
                started_at=datetime.now(timezone.utc).isoformat(),
            )

        entity_map, key_map = self._phase0()
        self._phase1(entity_map, key_map)

        if not historical_only and not self._stop:
            self._phase2(entity_map, key_map)

    def _handle_sigterm(self, signum, frame):
        logger.info("Signal %s received — stopping after current batch", signum)
        self._stop = True

    def _phase0(self):
        """Load entity map and key map. Returns (entity_map, key_map)."""
        logger.info("Phase 0: loading entity map and key map")
        entity_map = self._reader.load_entity_map()
        key_map = self._reader.load_key_map()
        total = self._reader.count_ts_kv()
        logger.info(
            "Phase 0 complete: %d entities, %d keys, %d ts_kv rows",
            len(entity_map),
            len(key_map),
            total,
        )
        now_ms = int(time.time() * 1000)
        self._tracker.update(phase="phase1", phase1_start_ts=now_ms)
        return entity_map, key_map

    def _phase1(self, entity_map, key_map):
        """Historical migration: all ts_kv rows → ScyllaDB, using parallel workers."""
        workers = self._cfg.workers if self._conn_factory else 1
        logger.info("Phase 1: historical migration started (workers=%d)", workers)

        progress = self._tracker.progress
        completed = set(progress.completed_entities)

        all_entities = list(self._reader.iter_distinct_entities())
        pending = [e for e in all_entities if e not in completed]
        logger.info(
            "Phase 1: %d entities total, %d pending, %d already done",
            len(all_entities), len(pending), len(completed),
        )

        lock = threading.Lock()

        def process_entity(entity_id_str: str):
            if self._stop:
                return

            entity_type = entity_map.get(entity_id_str)
            if entity_type is None:
                logger.warning("entity_id %s not found in entity_map, skipping", entity_id_str)
                with lock:
                    progress.skipped_rows += 1
                    self._tracker.update(skipped_rows=progress.skipped_rows)
                return

            conn = self._conn_factory()
            try:
                reader = PgReader(conn)
                batch_rows = []
                for row in reader.iter_ts_kv_for_entity(entity_id_str, self._cfg.batch_size):
                    if self._stop:
                        break
                    batch_rows.append(row)
                    if len(batch_rows) >= self._cfg.batch_size:
                        self._flush_ts_batch(batch_rows, entity_type, key_map)
                        with lock:
                            progress.migrated_rows += len(batch_rows)
                            self._tracker.update(migrated_rows=progress.migrated_rows)
                        batch_rows = []

                if batch_rows and not self._stop:
                    self._flush_ts_batch(batch_rows, entity_type, key_map)
                    with lock:
                        progress.migrated_rows += len(batch_rows)

                if not self._stop:
                    with lock:
                        progress.completed_entities.append(entity_id_str)
                        self._tracker.update(
                            migrated_rows=progress.migrated_rows,
                            last_entity_id=entity_id_str,
                            completed_entities=progress.completed_entities,
                        )
            finally:
                conn.close()

        if workers > 1:
            with ThreadPoolExecutor(max_workers=workers) as executor:
                futures = {executor.submit(process_entity, eid): eid for eid in pending}
                for future in as_completed(futures):
                    if self._stop:
                        for f in futures:
                            f.cancel()
                        break
                    exc = future.exception()
                    if exc:
                        logger.error("Entity %s failed: %s", futures[future], exc, exc_info=exc)
        else:
            for eid in pending:
                if self._stop:
                    break
                process_entity(eid)

        # Migrate latest values (single-threaded sequential scan)
        logger.info("Phase 1: migrating ts_kv_latest")
        latest_by_type = defaultdict(list)
        for row in self._reader.iter_ts_kv_latest():
            entity_id_str = str(row["entity_id"])
            entity_type = entity_map.get(entity_id_str)
            if entity_type is None:
                continue
            latest_by_type[entity_type].append(row)
            if len(latest_by_type[entity_type]) >= self._cfg.batch_size:
                self._writer.write_latest_batch(
                    latest_by_type[entity_type], entity_type, key_map, self._cast_fn
                )
                latest_by_type[entity_type] = []

        for entity_type, rows in latest_by_type.items():
            if rows:
                self._writer.write_latest_batch(rows, entity_type, key_map, self._cast_fn)

        logger.info("Phase 1 complete")
        self._tracker.update(phase="live_sync")

    def _flush_ts_batch(self, rows, entity_type, key_map):
        """Write a batch and its partitions to ScyllaDB. Thread-safe."""
        partitions = self._writer.write_ts_batch(
            rows, entity_type, key_map, self._partition_fn, self._cast_fn
        )
        self._writer.write_partitions(partitions)

    def _phase2(self, entity_map, key_map):
        """Live sync: poll ts_kv for rows newer than watermark."""
        logger.info("Phase 2: live sync started")
        progress = self._tracker.progress
        watermark = progress.watermark_ts or (progress.phase1_start_ts - 60_000)
        self._tracker.update(watermark_ts=watermark)

        while not self._stop:
            now_ms = int(time.time() * 1000)
            batch_rows_by_type = defaultdict(list)
            latest_by_type = defaultdict(list)

            for row in self._reader.iter_ts_kv_by_ts(watermark, self._cfg.batch_size):
                entity_id_str = str(row["entity_id"])
                entity_type = entity_map.get(entity_id_str)
                if entity_type is None:
                    continue
                batch_rows_by_type[entity_type].append(row)
                latest_by_type[entity_type].append(row)
                if row["ts"] > watermark:
                    watermark = row["ts"]

            for entity_type, rows in batch_rows_by_type.items():
                self._flush_ts_batch(rows, entity_type, key_map)

            for entity_type, rows in latest_by_type.items():
                self._writer.write_latest_batch(rows, entity_type, key_map, self._cast_fn)

            lag = now_ms - watermark
            self._tracker.update(watermark_ts=watermark)

            if lag < self._cfg.lag_threshold_ms:
                logger.info("LAG %dms < threshold %dms — ready for switchover", lag, self._cfg.lag_threshold_ms)

            time.sleep(self._cfg.live_sync_interval)
