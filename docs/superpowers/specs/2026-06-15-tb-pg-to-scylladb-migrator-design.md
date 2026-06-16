# ThingsBoard PostgreSQL → ScyllaDB Timeseries Migrator — Design Spec
Date: 2026-06-15  
Source verified: https://github.com/thingsboard/database-migrator (master branch, all Java files read)

---

## Overview

Python-based migration tool that moves ThingsBoard 4.3.1.1 CE timeseries data from PostgreSQL to ScyllaDB with minimal downtime. All services run in Docker (same compose network). Migration follows **TB Scenario 2** from the official migrator README: TB is first switched to Cassandra for new writes, then historical data is migrated in background.

**Migration mode:** Incremental continuous sync — TB stays running, ~30-60 sec downtime only for final TB restart.

**Scope:** Timeseries only. Attributes, entities, relations stay in PostgreSQL (hybrid mode).

**Key difference from Java tool:** Java migrator reads pg_dump files → generates SSTable files → bulk loads into Cassandra. Our Python tool connects to live PostgreSQL and ScyllaDB directly via CQL, which is better suited for Docker environments and supports continuous live sync.

---

## Verified Cassandra Schema (from schema-ts.cql + schema-ts-latest.cql)

```cql
CREATE KEYSPACE IF NOT EXISTS thingsboard
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

-- Timeseries data
CREATE TABLE IF NOT EXISTS thingsboard.ts_kv_cf (
    entity_type text,      -- 'DEVICE', 'CUSTOMER', 'TENANT', 'ASSET', etc.
    entity_id   timeuuid,  -- TB entity UUID (version-1 UUID)
    key         text,      -- resolved from ts_kv_dictionary (key_id → key name)
    partition   bigint,    -- truncated timestamp (first ms of month for MONTHS strategy)
    ts          bigint,    -- epoch milliseconds
    bool_v      boolean,
    str_v       text,
    long_v      bigint,
    dbl_v       double,
    json_v      text,
    PRIMARY KEY ((entity_type, entity_id, key, partition), ts)
);

-- Partition index
CREATE TABLE IF NOT EXISTS thingsboard.ts_kv_partitions_cf (
    entity_type text,
    entity_id   timeuuid,
    key         text,
    partition   bigint,
    PRIMARY KEY ((entity_type, entity_id, key), partition)
) WITH CLUSTERING ORDER BY (partition ASC)
  AND compaction = {'class': 'LeveledCompactionStrategy'};

-- Latest value per entity+key
CREATE TABLE IF NOT EXISTS thingsboard.ts_kv_latest_cf (
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
) WITH compaction = {'class': 'LeveledCompactionStrategy'};
```

---

## Source Schema (PostgreSQL)

```sql
-- Key name ↔ integer ID mapping
ts_kv_dictionary (key varchar(255) PK, key_id serial4)

-- Timeseries (entity_type NOT stored here — must be looked up)
ts_kv (entity_id uuid, key int4, ts int8, bool_v, str_v, long_v, dbl_v, json_v)

-- Latest values
ts_kv_latest (entity_id uuid, key int4, ts int8, bool_v, str_v, long_v, dbl_v, json_v)
```

---

## Entity Type Resolution

`ts_kv` stores only `entity_id` (UUID) — no `entity_type`. The Cassandra schema requires `entity_type` in every row's partition key. We must build a UUID → entity_type map by querying all entity tables at migration start (same logic as `RelatedEntitiesParser.java` but from live DB instead of dump file).

Entity tables to scan:

| PostgreSQL table  | entity_type value |
|-------------------|------------------|
| device            | DEVICE           |
| customer          | CUSTOMER         |
| tenant            | TENANT           |
| asset             | ASSET            |
| alarm             | ALARM            |
| dashboard         | DASHBOARD        |
| rule_chain        | RULE_CHAIN       |
| rule_node         | RULE_NODE        |
| tb_user           | USER             |
| entity_view       | ENTITY_VIEW      |
| widgets_bundle    | WIDGETS_BUNDLE   |
| widget_type       | WIDGET_TYPE      |
| tenant_profile    | TENANT_PROFILE   |
| device_profile    | DEVICE_PROFILE   |
| api_usage_state   | API_USAGE_STATE  |
| edge              | EDGE             |
| ota_package       | OTA_PACKAGE      |
| rpc               | RPC              |

Query: `SELECT id FROM <table>` for each table. If an entity_id is not found in any table, log warning and skip the row (matches Java tool's `EntityMissingException` handling).

---

## Partition Strategy (MONTHS)

Port of `NoSqlTsPartitionDate.java` MONTHS logic:

```python
from datetime import datetime, timezone

def months_partition(ts_ms: int) -> int:
    dt = datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc)
    # First millisecond of the month (truncate to day, set day=1)
    return int(datetime(dt.year, dt.month, 1, tzinfo=timezone.utc).timestamp() * 1000)
```

Supported strategies: MONTHS (default), DAYS, HOURS, MINUTES, YEARS, INDEFINITE (partition=0).

---

## Cast Strings to Numbers (castEnable flag)

Java tool has `-castEnable` option: if `str_v` looks like a number, store as `long_v` or `dbl_v` instead. Python tool supports this as `--cast-strings` CLI flag:

```python
def try_cast(str_v: str) -> tuple:
    try:
        return ('long_v', int(str_v))
    except ValueError:
        try:
            return ('dbl_v', float(str_v))
        except ValueError:
            return ('str_v', str_v)
```

---

## Architecture

```
┌────────────────────────────────────────────────────────┐
│                   Python Migrator                       │
│                                                         │
│  config.py    progress.py    partition.py               │
│                                                         │
│  ┌──────────────────────────────────────────────────┐  │
│  │               orchestrator.py                     │  │
│  │  Phase 0: Entity map   │  Phase 1: Historical    │  │
│  │  Phase 2: Live Sync    │  Switchover signal       │  │
│  └──────────────────────────────────────────────────┘  │
│          │                           │                  │
│   pg_reader.py                scylla_writer.py          │
└────────────────────────────────────────────────────────┘
         │                             │
  PostgreSQL (postgres:5432)    ScyllaDB (scylladb:9042)
  [Docker bridge network]       [Docker bridge network]
```

### File structure

```
TB_DB_Migrator/
├── docker-compose.scylla.yml    ← ScyllaDB + migrator services (overlay)
├── Dockerfile                   ← Migrator image
├── config.yaml                  ← Connection + tuning settings
├── requirements.txt
├── main.py                      ← CLI entry point (click)
├── migrator/
│   ├── __init__.py
│   ├── config.py                ← YAML loader + env var override
│   ├── pg_reader.py             ← PostgreSQL batch reader (named cursors)
│   ├── scylla_writer.py         ← ScyllaDB prepared-statement writer
│   ├── partition.py             ← Partition strategy implementations
│   ├── progress.py              ← JSON checkpoint file + resume logic
│   └── orchestrator.py         ← Phase coordinator
└── README.md                    ← O'zbekcha yo'riqnoma
```

---

## Migration Phases

### Phase 0 — Preload Maps (startup, ~seconds)

1. Query all entity tables → build `entity_map: {uuid_str: entity_type_str}` dict in memory.
2. Query `ts_kv_dictionary` → build `key_map: {key_id_int: key_name_str}` dict in memory.
3. Count rows in `ts_kv` → display total and estimated time.
4. Record `phase1_start_ts = now_ms` (used as Phase 2 watermark base).

### Phase 1 — Historical Migration

1. Use a server-side psycopg2 named cursor to iterate distinct `entity_id` values (avoids loading all UUIDs into memory for large datasets).
2. For each entity_id:
   - Look up `entity_type` from `entity_map`. If missing: log warning, skip.
   - Read `ts_kv` rows in batches of N (default 5,000), ordered by `ts ASC`.
   - For each row:
     - Resolve `key_name` from `key_map`
     - Compute `partition` via partition strategy
     - Apply cast if `--cast-strings` enabled
     - INSERT into `ts_kv_cf` (upsert — idempotent)
     - Track unique `(entity_type, entity_id, key, partition)` tuples for partitions table
   - After each batch: flush partition inserts to `ts_kv_partitions_cf`, save checkpoint.
3. After all entities: single-pass over `ts_kv_latest` → write to `ts_kv_latest_cf`.

**Batch size:** Default 5,000 rows. Auto-halved on ScyllaDB write timeout. Max retries: 3.

### Phase 2 — Live Sync

1. Watermark = `phase1_start_ts - 60_000` (60 sec safety margin before Phase 1 began).
2. Loop every 5 seconds:
   ```sql
   SELECT * FROM ts_kv WHERE ts > %s ORDER BY ts ASC LIMIT %s
   ```
   - Write rows to ScyllaDB (idempotent upserts — safe for duplicates).
   - Advance watermark to highest `ts` written.
   - Sync `ts_kv_latest` for affected entities.
   - Report: lag = `now_ms - watermark`.
3. When `lag < 30,000 ms`: print colored switchover instructions, keep syncing until SIGTERM.
4. On SIGTERM: flush current batch, log final watermark, exit cleanly.

---

## Docker Integration

Add to existing stack via overlay file:

```yaml
# docker-compose.scylla.yml
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

Run command:
```bash
docker compose -f docker-compose.yml -f docker-compose.scylla.yml up -d scylladb
docker compose -f docker-compose.yml -f docker-compose.scylla.yml run --rm tb-migrator python main.py init-schema
docker compose -f docker-compose.yml -f docker-compose.scylla.yml run --rm tb-migrator python main.py start
```

---

## CLI Interface

```bash
python main.py init-schema                    # ScyllaDB keyspace + 3 jadval yaratish
python main.py start                          # To'liq migratsiya (Phase 0+1+2)
python main.py start --resume                 # Checkpoint dan davom ettirish
python main.py start --historical-only        # Faqat Phase 0+1 (live sync yo'q)
python main.py start --cast-strings           # str_v ni number ga cast qilish
python main.py start --partitioning DAYS      # Partition strategiyasini o'zgartirish
python main.py status                         # Checkpoint holati ko'rish
```

**Terminal output (Rich library):**
```
TB PostgreSQL → ScyllaDB Migrator v1.0
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
 Jami qatorlar    : 142,847,392
 Ko'chirildi      : 98,234,100  (68.7%)
 Tezlik           : 45,230 qator/sek
 Qolgan vaqt      : ~14 daqiqa
 [████████████████░░░░░░░░] 68.7%

 FAZA 2 – LIVE SYNC (Faol)
 Watermark lag    : 8 soniya
 Yangi qatorlar   : 1,240/min

 ✅ TAYYOR: Lag 30 soniyadan kam. Switchover qiling!
```

---

## Switchover Procedure (TB Scenario 2)

After Phase 2 signals readiness:

**1. ThingsBoard ni to'xtatish:**
```bash
docker compose stop thingsboard-ce
```

**2. `docker-compose.yml` dagi `thingsboard-ce` servisiga qo'shish:**
```yaml
environment:
  DATABASE_TS_TYPE: cassandra
  TS_KV_PARTITIONING: MONTHS
  CASSANDRA_URL: scylladb:9042
  CASSANDRA_CLUSTER_NAME: TB Cluster
  CASSANDRA_USE_CREDENTIALS: "false"
  CASSANDRA_KEYSPACE_NAME: thingsboard
  # O'chirish: SQL_TTL_TS_ENABLED, SQL_TTL_TS_TS_KEY_VALUE_TTL
```

**3. TB ni qayta ishga tushirish:**
```bash
docker compose -f docker-compose.yml -f docker-compose.scylla.yml up -d thingsboard-ce
```

**4. Migrator ni to'xtatish:**
```bash
docker compose -f docker-compose.yml -f docker-compose.scylla.yml stop tb-migrator
```

---

## Checkpoint Format (`migration_progress.json`)

```json
{
  "phase": "live_sync",
  "phase1_start_ts": 1718445600000,
  "last_entity_id": "550e8400-e29b-11d4-a716-446655440000",
  "last_entity_ts": 1718440000000,
  "watermark_ts": 1718445570000,
  "migrated_rows": 98234100,
  "skipped_rows": 142,
  "started_at": "2026-06-15T10:00:00Z",
  "partitioning": "MONTHS",
  "cast_strings": false
}
```

---

## Error Handling

| Xatolik | Yechim |
|---------|--------|
| Network uzilishi | Exponential backoff retry (3 urinish, 2s→8s→30s) |
| ScyllaDB WriteTimeout | Batch hajmini 50% kamaytirish, qayta urinish |
| entity_id entity_map da yo'q | `migration_errors.log` ga yozib, qatorni o'tkazib yuborish |
| key_id key_map da yo'q | key_id ni string sifatida ishlatish, warning log |
| Migrator kutilmaganda to'xtasa | `--resume` bilan checkpoint dan davom ettirish |

---

## Dependencies

```
psycopg2-binary>=2.9.9    # PostgreSQL driver (named cursor support)
cassandra-driver>=3.29.1  # Cassandra/ScyllaDB driver
scylla-driver>=3.26       # ScyllaDB-optimized fork (preferred over cassandra-driver)
pyyaml>=6.0.2             # Config file parser
rich>=13.7                # Terminal progress bar, tables, colors
click>=8.1.7              # CLI argument parsing
```

> Note: `scylla-driver` is a drop-in replacement for `cassandra-driver` with ScyllaDB-specific optimizations (token-aware routing, shard-aware connections). Use it if available.

---

## TTL Notes

- PostgreSQL TTL: `SQL_TTL_TS_TS_KEY_VALUE_TTL: 63072000` (2 yil, soniyalarda) — TB o'zi qatorlarni tozalaydi.
- ScyllaDB/Cassandra: TB `DATABASE_TS_TYPE=cassandra` rejimida yangi yoziladigan qatorlarga avtomatik TTL qo'ymaydi; TB o'zi boshqaradi.
- Migratsiya paytida historical qatorlarga TTL qo'yilmaydi — to'liq historical data saqlanadi.
- TB Cassandra TTL sozlamasi (agar kerak bo'lsa): `CASSANDRA_QUERY_TS_KEY_VALUE_TTL` env var.

---

## Out of Scope

- `ts_kv` dan boshqa jadvallar (attribute_kv, entities, relations, audit_log) — PostgreSQL da qoladi
- SSTable generation va `sstableloader` (Docker uchun kerak emas)
- Multi-node ScyllaDB cluster setup (keyspace replication_factor>1)
- TimescaleDB hypertable support (TB 4.3.1.1 CE standart PostgreSQL ishlatadi)
- Windows-native deployment (Docker orqali ishlaydi)
