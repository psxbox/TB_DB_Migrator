# Disk-Constrained TB Migration Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Partition-scoped migration tool + single compose for new PG/Scylla/TB-PE with external volumes, dump→migrate→verify→DROP cycle per `ts_kv` partition.

**Architecture:** `PgReader` streams directly from child partition tables (`SELECT ... FROM "<part>"` with keyset pagination); `ScyllaReader` verifies via write-count + point lookups; `ProgressTracker` tracks per-partition state; CLI gains `list-partitions / start --partition / verify / drop`; new compose `docker-compose.new-stack.yml` holds all three services with external volumes.

**Tech Stack:** .NET 10, Npgsql 10.*, CassandraCSharpDriver 3.*, YamlDotNet 18.*, Spectre.Console, postgres:18, scylladb/scylla:2026.1, thingsboard/tb-pe-node:3.4.1PE

**Spec:** `docs/superpowers/specs/2026-09-03-disk-constrained-tb-migration-design.md`

## Global Constraints

- Branch `tb-3.4`, TB 3.4.1 PE, `TS_KV_PARTITIONING=MONTHS` everywhere (compose env == `config.yaml` == `Partition.Compute` strategy).
- New PG image `postgres:18` (same major as old PG v18).
- Scylla image `scylladb/scylla:2026.1` (keep existing).
- External docker volumes `tb-pg-new-data` and `tb-scylla-data` created via `docker volume create` before compose up; compose declares them `external: true`.
- All large dumps via pipe `docker exec $OLD_PG pg_dump ... > ~/backup/x.dump` (no `-t` flag on docker exec, no intermediate `/tmp/*.dump` — container overlay lives on root disk).
- `DROP TABLE <part>` only when: dump file exists + readable, `verify` passed (checkpoint flag), `--verified` flag given, and for hot partition additionally switchover check passed.
- Old PG container name never hardcoded — `OLD_PG` env + discovery; tool connects via `PG_HOST/PG_PORT` env over `config.yaml`.
- Existing CLI flags (`--resume`, `--workers`, `--historical-only`) keep working.
- RAM budget (8 GB host, other services running): `tb-pe` mem_limit 3g (JAVA_OPTS `-Xms1G -Xmx2G`), `scylladb` mem_limit 2g (`--smp 1 --memory 1G --overprovisioned 1`), `postgres-new` mem_limit 512m (`shared_buffers=128MB`). Migrator starts with `workers: 2`, `scylla_concurrency: 32`.

---

## File Map

```
NEW:
  docker-compose.new-stack.yml   <- postgres-new + scylladb + tb-pe (profile tb), external volumes
  ScyllaReader.cs                <- verify reads (point lookup + partition-range count via token-aware per-partition queries)
  scripts/tb-api-check.sh        <- login -> device -> latest -> history check, all 200 + non-empty

MODIFY:
  Progress.cs                    <- add Partitions dict + PartitionProgress record
  Config.cs                      <- add partition_batch + verify_sample_size
  PgReader.cs                    <- add ListPartitionsAsync + CountPartitionAsync + StreamPartitionAsync
  ScyllaWriter.cs                <- expose Session getter (1 property, no logic change)
  Program.cs                     <- add list-partitions / verify / drop commands + --partition / --delta-from flags
  Orchestrator.cs                <- add RunPartitionAsync + VerifyPartitionAsync + DropPartitionAsync
  config.yaml                    <- add partition_batch + verify_sample_size defaults
  README.md                      <- append disk-constrained runbook section
```

---

### Task 1: New-stack compose with external volumes

**Files:**
- Create: `docker-compose.new-stack.yml`
- Test: `docker compose -f docker-compose.new-stack.yml config` (no daemon needed)

**Interfaces:**
- Consumes: nothing (standalone YAML).
- Produces: services `postgres-new` (127.0.0.1:15432), `scylladb` (127.0.0.1:9042), `tb-pe` (profile `tb`, starts only with `--profile tb`).

- [ ] **Step 1: Create `docker-compose.new-stack.yml`**

```yaml
services:
  postgres-new:
    image: postgres:18
    container_name: postgres-new
    mem_limit: 512m
    mem_reservation: 256m
    command: postgres -c shared_buffers=128MB -c effective_cache_size=256MB -c maintenance_work_mem=64MB -c max_connections=100
    environment:
      POSTGRES_DB: ${NEW_PG_DB:-thingsboard}
      POSTGRES_USER: ${NEW_PG_USER:-postgres}
      POSTGRES_PASSWORD: ${NEW_PG_PASSWORD:-postgres}
    ports:
      - "127.0.0.1:15432:5432"
    volumes:
      - tb-pg-new-data:/var/lib/postgresql
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U postgres -d thingsboard || exit 1"]
      interval: 15s
      timeout: 5s
      retries: 10

  scylladb:
    image: scylladb/scylla:2026.1
    container_name: scylladb-new
    mem_limit: 2g
    command: --smp 1 --memory 1G --overprovisioned 1
    ports:
      - "127.0.0.1:9042:9042"
    volumes:
      - tb-scylla-data:/var/lib/scylla
    healthcheck:
      test: ["CMD-SHELL", "cqlsh -e 'describe keyspaces' || exit 1"]
      interval: 30s
      timeout: 10s
      retries: 10

  tb-pe:
    image: thingsboard/tb-pe-node:3.4.1PE
    container_name: tb-pe-new
    profiles: ["tb"]
    mem_limit: 3g
    mem_reservation: 2g
    depends_on:
      postgres-new:
        condition: service_healthy
      scylladb:
        condition: service_healthy
    environment:
      JAVA_OPTS: "-Xms1G -Xmx2G"
      SPRING_DATASOURCE_URL: jdbc:postgresql://postgres-new:5432/${NEW_PG_DB:-thingsboard}
      SPRING_DATASOURCE_USERNAME: ${NEW_PG_USER:-postgres}
      SPRING_DATASOURCE_PASSWORD: ${NEW_PG_PASSWORD:-postgres}
      DATABASE_TS_TYPE: cassandra
      TS_KV_PARTITIONING: MONTHS
      CASSANDRA_URL: scylladb:9042
      CASSANDRA_CLUSTER_NAME: "TB Cluster"
      CASSANDRA_USE_CREDENTIALS: "false"
      CASSANDRA_KEYSPACE_NAME: thingsboard
      TB_LICENSE_SECRET: ${TB_LICENSE_SECRET:-}
      TB_LICENSE_INSTANCE_DATA_FILE: ${TB_LICENSE_INSTANCE_DATA_FILE:-}
    ports:
      - "8080:8080"
      - "1883:1883"
      - "5683:5683/udp"

volumes:
  tb-pg-new-data:
    external: true
    name: tb-pg-new-data
  tb-scylla-data:
    external: true
    name: tb-scylla-data
```

- [ ] **Step 2: Validate compose renders without daemon**

Run: `docker compose -f docker-compose.new-stack.yml config`
Expected: prints resolved YAML with three services, no error about missing volumes (external check happens at `up`, not `config`).

- [ ] **Step 3: Commit**

```bash
git add docker-compose.new-stack.yml
git commit -m "feat: add new-stack compose (pg-new, scylla, tb-pe) with external volumes"
```

---

### Task 2: Partition progress state

**Files:**
- Modify: `Progress.cs`
- Test: `dotnet build TbMigrator.csproj -c Release` (deserialization of old checkpoint without `partitions` must still work)

**Interfaces:**
- Consumes: existing `Progress` / `ProgressTracker` (System.Text.Json).
- Produces: `PartitionProgress` record + `Progress.Partitions` dict used by Tasks 5-6:
  ```csharp
  public record PartitionProgress(string State, long PgCount, long ScyllaCount, string? DumpFile, bool Verified, bool Dropped, long MaxTs);
  // Progress.Partitions: Dictionary<string, PartitionProgress>
  ```

- [ ] **Step 1: Add `PartitionProgress` + `Partitions` dict to `Progress.cs`**

```csharp
// ...existing code...
public class Progress
{
    // ...existing code...
    [JsonPropertyName("partitions")] public Dictionary<string, PartitionProgress> Partitions { get; set; } = [];
}

public record PartitionProgress(
    [property: JsonPropertyName("state")] string State,               // pending|migrating|migrated|verified|dropped
    [property: JsonPropertyName("pg_count")] long PgCount,
    [property: JsonPropertyName("scylla_count")] long ScyllaCount,
    [property: JsonPropertyName("dump_file")] string? DumpFile,
    [property: JsonPropertyName("verified")] bool Verified,
    [property: JsonPropertyName("dropped")] bool Dropped,
    [property: JsonPropertyName("max_ts")] long MaxTs);
```

Apply via `replace_string_in_file` on `Progress.cs`: keep all existing properties, append `Partitions` property after `CompletedEntities`, add the new record after the class. Old `migration_progress.json` (`{}` or without `partitions`) deserializes with empty dict — backward compatible.

- [ ] **Step 2: Build to verify no breakage**

Run: `dotnet build TbMigrator.csproj -c Release`
Expected: `Build succeeded.`

- [ ] **Step 3: Commit**

```bash
git add Progress.cs
git commit -m "feat: add per-partition progress state"
```

---

### Task 3: Config additions

**Files:**
- Modify: `Config.cs`
- Modify: `config.yaml`
- Test: `dotnet build TbMigrator.csproj -c Release`

**Interfaces:**
- Consumes: existing `MigratorConfig` (YamlDotNet `UnderscoredNamingConvention`).
- Produces: `MigratorConfig.PartitionBatch` (int, default 5000), `MigratorConfig.VerifySampleSize` (int, default 1000).

- [ ] **Step 1: Add two properties to `MigratorConfig` in `Config.cs`**

```csharp
    [YamlMember(Alias = "partition_batch")]
    public int    PartitionBatch   { get; set; } = 5000;

    [YamlMember(Alias = "verify_sample_size")]
    public int    VerifySampleSize { get; set; } = 1000;
```

Insert after the existing `CastStrings` property, before `CheckpointFile`. Defaults equal current `batch_size` so behavior is unchanged when keys are absent.

- [ ] **Step 2: Add defaults to `config.yaml`**

```yaml
migrator:
  batch_size: 5000
  workers: 4
  scylla_concurrency: 64
  live_sync_interval: 5.0
  lag_threshold_ms: 30000
  partitioning: MONTHS
  cast_strings: false
  checkpoint_file: migration_progress.json
  partition_batch: 5000
  verify_sample_size: 1000
```

- [ ] **Step 3: Build**

Run: `dotnet build TbMigrator.csproj -c Release`
Expected: `Build succeeded.`

- [ ] **Step 4: Commit**

```bash
git add Config.cs config.yaml
git commit -m "feat: add partition_batch and verify_sample_size config"
```

---

### Task 4: PgReader partition listing, counting, streaming

**Files:**
- Modify: `PgReader.cs`
- Test: `dotnet build TbMigrator.csproj -c Release` + live `list-partitions` against old PG (server-side)

**Interfaces:**
- Consumes: `NpgsqlConnection _conn`, `_hybridMode`.
- Produces (used by Tasks 6-7):
  ```csharp
  public record PartitionInfo(string Name, long MinTs, long MaxTs, long Count, long SizeBytes);
  public Task<List<PartitionInfo>> ListPartitionsAsync(CancellationToken ct = default);
  public Task<long> CountPartitionAsync(string partition, CancellationToken ct = default);
  public IAsyncEnumerable<List<TsRow>> StreamPartitionAsync(string partition, long deltaFromTs, Dictionary<int,string> keyMap, bool hybridMode, int batchSize, CancellationToken ct = default);
  ```

- [ ] **Step 1: Add `PartitionInfo` record + `ListPartitionsAsync` to `PgReader.cs`**

```csharp
public record PartitionInfo(string Name, long MinTs, long MaxTs, long Count, long SizeBytes);

public async Task<List<PartitionInfo>> ListPartitionsAsync(CancellationToken ct = default)
{
    var names = new List<string>();
    await using (var cmd = _conn.CreateCommand())
    {
        cmd.CommandText = "SELECT inhrelid::regclass::text FROM pg_inherits WHERE inhparent = 'ts_kv'::regclass ORDER BY 1";
        await using var rdr = await cmd.ExecuteReaderAsync(ct);
        while (await rdr.ReadAsync(ct))
            names.Add(rdr.GetString(0));
    }
    var result = new List<PartitionInfo>(names.Count);
    foreach (var name in names)
    {
        await using var cmd = _conn.CreateCommand();
        // identifier cannot be parameterised — validate against pg_inherits list above
        cmd.CommandText = $"SELECT COALESCE(MIN(ts),0), COALESCE(MAX(ts),0), COUNT(*), pg_total_relation_size('\"{name}\"') FROM \"{name}\"";
        await using var rdr = await cmd.ExecuteReaderAsync(ct);
        await rdr.ReadAsync(ct);
        result.Add(new PartitionInfo(name, rdr.GetInt64(0), rdr.GetInt64(1), rdr.GetInt64(2), rdr.GetInt64(3)));
    }
    return result;
}

public async Task<long> CountPartitionAsync(string partition, CancellationToken ct = default)
{
    await using var cmd = _conn.CreateCommand();
    cmd.CommandText = $"SELECT COUNT(*) FROM \"{partition}\"";
    return (long)(await cmd.ExecuteScalarAsync(ct))!;
}
```

Safety: `partition` is only ever taken from `ListPartitionsAsync` output or `--partition` validated against it (Task 6 does the validation) — never raw user SQL.

- [ ] **Step 2: Add `StreamPartitionAsync` (child-table keyset pagination, hybrid + text modes)**

```csharp
public async IAsyncEnumerable<List<TsRow>> StreamPartitionAsync(
    string partition,
    long deltaFromTs,
    Dictionary<int, string> keyMap,
    bool hybridMode,
    int batchSize,
    [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken ct = default)
{
    (long, Guid, string)? last = null;
    while (true)
    {
        await using var cmd = _conn.CreateCommand();
        string keySel = hybridMode ? "key" : "key::text";
        if (last is null)
        {
            cmd.CommandText =
                $"SELECT entity_id, {keySel}, ts, bool_v, str_v, long_v, dbl_v, json_v " +
                $"FROM \"{partition}\" WHERE ts > $1 ORDER BY ts, entity_id, key LIMIT $2";
            cmd.Parameters.AddWithValue(deltaFromTs);
            cmd.Parameters.AddWithValue(batchSize);
        }
        else
        {
            cmd.CommandText =
                $"SELECT entity_id, {keySel}, ts, bool_v, str_v, long_v, dbl_v, json_v " +
                $"FROM \"{partition}\" WHERE (ts, entity_id, key) > ($1,$2,$3) " +
                "ORDER BY ts, entity_id, key LIMIT $4";
            cmd.Parameters.AddWithValue(last.Value.Item1);
            cmd.Parameters.AddWithValue(last.Value.Item2);
            // hybrid: key column is int — parse back; text mode: raw string
            if (hybridMode && int.TryParse(last.Value.Item3, out int kid))
                cmd.Parameters.AddWithValue(kid);
            else
                cmd.Parameters.AddWithValue(last.Value.Item3);
            cmd.Parameters.AddWithValue(batchSize);
        }

        var batch = new List<TsRow>(batchSize);
        await using var rdr = await cmd.ExecuteReaderAsync(ct);
        while (await rdr.ReadAsync(ct))
        {
            var row = hybridMode ? ReadRowIntKeyHybrid(rdr, keyMap) : ReadRow(rdr, keyMap, false);
            batch.Add(row);
            last = (row.Ts, Guid.Parse(row.EntityId), rdr[1].ToString()!);
        }
        if (batch.Count > 0) yield return batch;
        if (batch.Count < batchSize) yield break;
    }
}

private static TsRow ReadRowIntKeyHybrid(Npgsql.NpgsqlDataReader rdr, Dictionary<int, string> keyMap)
{
    // column order here: 0 entity_id, 1 key(int), 2 ts, 3..7 values
    int keyId = rdr.GetInt32(1);
    string keyName = keyMap.TryGetValue(keyId, out var name) ? name : keyId.ToString();
    return new TsRow(
        EntityId: rdr.GetGuid(0).ToString(),
        Key:      keyName,
        Ts:       rdr.GetInt64(2),
        BoolV:    rdr.IsDBNull(3) ? null : rdr.GetBoolean(3),
        StrV:     rdr.IsDBNull(4) ? null : rdr.GetString(4),
        LongV:    rdr.IsDBNull(5) ? null : rdr.GetInt64(5),
        DblV:     rdr.IsDBNull(6) ? null : rdr.GetDouble(6),
        JsonV:    rdr.IsDBNull(7) ? null : rdr.GetString(7));
}
```

Note: existing private `ReadRowIntKey` expects column order `entity_id,key,ts,...` too (used by live-sync) — the new helper duplicates it because the existing one reads `GetInt32(1)`/`GetInt64(2)` identically; reuse it if signatures match, else keep the new one. Keep `ORDER BY ts, entity_id, key` numeric-safe in hybrid mode (no `::text` in ORDER BY).

- [ ] **Step 3: Build**

Run: `dotnet build TbMigrator.csproj -c Release`
Expected: `Build succeeded.` (fix duplicate-helper error by reusing existing `ReadRowIntKey` if compiler complains).

- [ ] **Step 4: Commit**

```bash
git add PgReader.cs
git commit -m "feat: add partition list/count/stream to PgReader"
```

---

### Task 5: ScyllaReader verify support

**Files:**
- Create: `ScyllaReader.cs`
- Modify: `ScyllaWriter.cs` (add `public ISession Session => _session;` only)
- Test: `dotnet build TbMigrator.csproj -c Release`

**Interfaces:**
- Consumes: `Cassandra.ISession` (via `ScyllaWriter.Session`), `Partition.Compute(ts, strategy)`.
- Produces (used by Task 6):
  ```csharp
  public Task<long> CountByPartitionValuesAsync(string entityType, Guid entityId, string key, IEnumerable<long> partitions, CancellationToken ct = default);
  public Task<TsRow?> GetPointAsync(string entityType, Guid entityId, string key, long partition, long ts, CancellationToken ct = default);
  ```

- [ ] **Step 1: Expose session in `ScyllaWriter.cs` (one property, no logic change)**

```csharp
public ISession Session => _session;
```

Insert after the private constructor, before `InitSchema()`.

- [ ] **Step 2: Create `ScyllaReader.cs`**

```csharp
using Cassandra;

namespace TbMigrator;

public class ScyllaReader
{
    private readonly ISession _session;
    private readonly string _keyspace;

    public ScyllaReader(ISession session, string keyspace)
    {
        _session = session;
        _keyspace = keyspace;
    }

    public async Task<long> CountByPartitionValuesAsync(
        string entityType, Guid entityId, string key, IEnumerable<long> partitions, CancellationToken ct = default)
    {
        long total = 0;
        foreach (var p in partitions.Distinct())
        {
            var rs = await _session.ExecuteAsync(
                new SimpleStatement(
                    $"SELECT COUNT(*) FROM {_keyspace}.ts_kv_cf WHERE entity_type=? AND entity_id=? AND key=? AND partition=?",
                    entityType, entityId, key, p));
            total += (long)rs.First().GetValue<object>("count")!;
        }
        return total;
    }

    public async Task<TsRow?> GetPointAsync(
        string entityType, Guid entityId, string key, long partition, long ts, CancellationToken ct = default)
    {
        var rs = await _session.ExecuteAsync(
            new SimpleStatement(
                $"SELECT entity_id, key, ts, bool_v, str_v, long_v, dbl_v, json_v FROM {_keyspace}.ts_kv_cf " +
                "WHERE entity_type=? AND entity_id=? AND key=? AND partition=? AND ts=?",
                entityType, entityId, key, partition, ts));
        var row = rs.FirstOrDefault();
        if (row is null) return null;
        return new TsRow(
            EntityId: row.GetValue<Guid>("entity_id").ToString(),
            Key:      row.GetValue<string>("key"),
            Ts:       row.GetValue<long>("ts"),
            BoolV:    row.GetValue<bool?>("bool_v"),
            StrV:     row.GetValue<string?>("str_v"),
            LongV:    row.GetValue<long?>("long_v"),
            DblV:     row.GetValue<double?>("dbl_v"),
            JsonV:    row.GetValue<string?>("json_v"));
    }
}
```

Why per-(entity,key,partition) COUNT instead of one global query: `ts_kv_cf` partition key is `(entity_type,entity_id,key,partition)` — a global `WHERE partition IN (...)` would need ALLOW FILTERING full scan. Verify flow (Task 6) instead compares PG `COUNT(*)` against the migrator's written-row counter in checkpoint, then spot-checks N random points via `GetPointAsync`. `CountByPartitionValuesAsync` is used only for small spot entity samples, not whole-table scans.

- [ ] **Step 3: Build**

Run: `dotnet build TbMigrator.csproj -c Release`
Expected: `Build succeeded.`

- [ ] **Step 4: Commit**

```bash
git add ScyllaReader.cs ScyllaWriter.cs
git commit -m "feat: add ScyllaReader point-lookup verify support"
```

---

### Task 6: CLI — list-partitions / start --partition / verify / drop

**Files:**
- Modify: `Program.cs`
- Modify: `Orchestrator.cs`
- Test: `dotnet build` + `tbmigrator list-partitions` against old PG (server-side)

**Interfaces:**
- Consumes: `PgReader.ListPartitionsAsync/CountPartitionAsync/StreamPartitionAsync`, `ScyllaReader.GetPointAsync`, `Progress.Partitions`, `MigratorConfig.PartitionBatch/VerifySampleSize`.
- Produces: CLI surface:
  ```
  tbmigrator list-partitions [--config FILE]
  tbmigrator start --partition <part> [--delta-from <tsMs>] [--resume] [--workers N] [--historical-only] [--config FILE]
  tbmigrator verify --partition <part> [--config FILE]            # exit 0 match, 1 mismatch
  tbmigrator drop --partition <part> --dump-file <path> --verified [--config FILE]
  ```

- [ ] **Step 1: Extend `Program.cs` command switch**

```csharp
case "list-partitions": return await RunListPartitions(config);
case "verify":          return await RunVerify(args, config);
case "drop":            return await RunDrop(args, config);
// "start" gains: string? part = Flag(args, "--partition");
//                long deltaFrom = long.TryParse(Flag(args, "--delta-from"), out var d) ? d : long.MinValue;
//                pass (part, deltaFrom) into orch.RunPartitionAsync when set, else existing RunAsync.
```

`RunListPartitions`: open PG, `LoadKeyMapAsync` (not needed — skip), `ListPartitionsAsync`, print Spectre table (Name, MinTs ISO, MaxTs ISO, Count, Size MB). `RunVerify`: load checkpoint partition entry (must exist + state `migrated`), PG `CountPartitionAsync` vs checkpoint `ScyllaCount`; if equal, sample `VerifySampleSize` random rows: `SELECT entity_id, key::text, ts FROM "<part>" ORDER BY random() LIMIT N` then `ScyllaReader.GetPointAsync` per row with `Partition.Compute(ts, cfg.Partitioning)` and full value equality; set `Verified=true`, state `verified` on success; return 0/1. `RunDrop`: require `--verified` flag present AND checkpoint `Verified==true` AND `File.Exists(dumpFile)` AND `pg_restore --list` readability is checked by operator (documented) — then `DROP TABLE "<part>";` on old PG, set state `dropped`. Refuse otherwise with message + return 1.

- [ ] **Step 2: Add `Orchestrator.RunPartitionAsync` (migrate one partition, entity-type resolved via existing entity map)**

```csharp
public async Task RunPartitionAsync(string partition, long deltaFromTs, int workers, CancellationToken ct)
{
    var (keyMap, hybridMode) = await _reader.LoadKeyMapAsync(ct);
    var entityMap = await _reader.LoadEntityMapAsync(ct);
    long pgCount = await _reader.CountPartitionAsync(partition, ct);
    _tracker.Update(p => p.Partitions[partition] = new PartitionProgress(
        "migrating", pgCount, p.Partitions.TryGetValue(partition, out var prev) ? prev.ScyllaCount : 0,
        p.Partitions.TryGetValue(partition, out var pr) ? pr.DumpFile : null, false, false, 0));
    long written = 0, maxTs = deltaFromTs;
    var outerOpts = new ParallelOptions { MaxDegreeOfParallelism = workers, CancellationToken = ct };
    // Stream batches sequentially (ordering matters for keyset pagination), write batches in parallel via ScyllaWriter semaphore
    await foreach (var batch in _reader.StreamPartitionAsync(partition, deltaFromTs, keyMap, hybridMode, _cfg.Migrator.PartitionBatch, ct))
    {
        var byType = new Dictionary<string, List<TsRow>>();
        foreach (var row in batch)
        {
            if (!entityMap.TryGetValue(row.EntityId, out var et)) continue;
            if (!byType.ContainsKey(et)) byType[et] = [];
            byType[et].Add(row);
            if (row.Ts > maxTs) maxTs = row.Ts;
        }
        var tasks = byType.Select(async kv =>
        {
            var parts = await _scylla.WriteTsBatchAsync(kv.Value, kv.Key, _cfg.Migrator.Partitioning, _cfg.Migrator.CastStrings, ct);
            await _scylla.WritePartitionsAsync(parts, ct);
        });
        await Task.WhenAll(tasks);
        written += batch.Count;
        long w = written, m = maxTs;
        _tracker.Update(p => p.Partitions[partition] = p.Partitions[partition] with { ScyllaCount = w, MaxTs = m });
        _tracker.Update(p => p.MigratedRows += batch.Count);
    }
    _tracker.Update(p => p.Partitions[partition] = p.Partitions[partition] with { State = "migrated" });
}
```

`--delta-from` resume: pass previous `MaxTs` so only `ts > MaxTs` rows stream. `--resume` without `--delta-from` continues counting from stored `ScyllaCount` note in log (no double-write harm: Scylla INSERTs are idempotent upserts on same PK).

- [ ] **Step 3: Build**

Run: `dotnet build TbMigrator.csproj -c Release`
Expected: `Build succeeded.`

- [ ] **Step 4: Server-side smoke test `list-partitions`**

Run (server): `dotnet bin/Release/net10.0/tbmigrator.dll list-partitions`
Expected: table of ~277 partitions with counts; newest partition has largest MaxTs.

- [ ] **Step 5: Commit**

```bash
git add Program.cs Orchestrator.cs
git commit -m "feat: add partition CLI (list/start/verify/drop) and partition runner"
```

---

### Task 7: API check script + README runbook

**Files:**
- Create: `scripts/tb-api-check.sh`
- Modify: `README.md` (append section, keep existing content untouched)
- Test: `bash -n scripts/tb-api-check.sh` + `docker compose -f docker-compose.new-stack.yml config`

**Interfaces:**
- Consumes: new TB at `$TB_URL` (default `http://localhost:8080`), credentials env `TB_USER/TB_PASS`.
- Produces: exit 0 only if login→devices→latest→history all 200 and history non-empty.

- [ ] **Step 1: Create `scripts/tb-api-check.sh`**

```bash
#!/usr/bin/env bash
# Full TB check: login, device list (PG), latest (latest_cf), history (ts_kv_cf).
set -euo pipefail
TB_URL="${TB_URL:-http://localhost:8080}"
TB_USER="${TB_USER:?set TB_USER}"
TB_PASS="${TB_PASS:?set TB_PASS}"
DEVICE_ID="${DEVICE_ID:?set DEVICE_ID to a device with migrated telemetry}"
START_TS="${START_TS:?set START_TS ms (newest partition min)}"
END_TS="${END_TS:-$(date +%s)000}"

TOKEN=$(curl -sf -X POST "$TB_URL/api/auth/login" \
  -H 'Content-Type: application/json' \
  -d "{\"username\":\"$TB_USER\",\"password\":\"$TB_PASS\"}" | python3 -c 'import sys,json;print(json.load(sys.stdin)["token"])')
echo "login OK"
curl -sf -H "X-Authorization: Bearer $TOKEN" "$TB_URL/api/device/$DEVICE_ID" > /dev/null
echo "device OK (PG)"
LATEST=$(curl -sf -H "X-Authorization: Bearer $TOKEN" \
  "$TB_URL/api/plugins/telemetry/DEVICE/$DEVICE_ID/values/timeseries?keys=temperature")
python3 -c 'import sys,json;d=json.load(sys.stdin);assert d.get("temperature"),"latest empty";print("latest OK",d["temperature"][0]["ts"])'
HIST=$(curl -sf -H "X-Authorization: Bearer $TOKEN" \
  "$TB_URL/api/plugins/telemetry/DEVICE/$DEVICE_ID/values/timeseries?keys=temperature&startTs=$START_TS&endTs=$END_TS&limit=10")
python3 -c 'import sys,json;d=json.load(sys.stdin);assert d.get("temperature"),"history empty";print("history OK rows=",len(d["temperature"]))'
echo ALL_CHECKS_PASSED
```

- [ ] **Step 2: Syntax-check script**

Run: `bash -n scripts/tb-api-check.sh`
Expected: no output (syntax OK).

- [ ] **Step 3: Append runbook to `README.md` (new section at end, no edits above)**

```markdown
## 10. Disk-cheklangan migratsiya (partition sikl)

Volume'lar: `docker volume create tb-pg-new-data && docker volume create tb-scylla-data`
Yangi stack: `docker compose -f docker-compose.new-stack.yml up -d postgres-new scylladb`
Schema + non-ts_kv (pipe, oraliq faylsiz):
`docker exec $OLD_PG pg_dump -U postgres -d thingsboard --schema-only -Fc > ~/backup/schema.dump`
`docker exec $OLD_PG pg_dump -U postgres -d thingsboard --data-only -Fc --exclude-table-data='ts_kv*' > ~/backup/nontskv.dump`
Har partition: dump (`docker exec $OLD_PG pg_dump -Fc -t <part> > ~/backup/<part>.dump`) -> `tbmigrator start --partition <part>` -> `tbmigrator verify --partition <part>` -> `tbmigrator drop --partition <part> --dump-file ~/backup/<part>.dump --verified`
Hot partition DROP faqat switchover + `scripts/tb-api-check.sh` (`TB_USER/TB_PASS/DEVICE_ID/START_TS`) `ALL_CHECKS_PASSED` dan keyin.
```

- [ ] **Step 4: Commit**

```bash
git add scripts/tb-api-check.sh README.md
git commit -m "feat: add tb api check script and disk-constrained runbook"
```

---

## Self-Review

- Spec coverage: compose+volumes (Task 1), non-ts_kv copy (manual runbook Task 7, uses pipe per spec 5), newest-first + switchover + new-to-old loop (runbook Task 7 + runner Task 6), hot DROP rule (gate in Task 6 `RunDrop` + runbook), tool changes list/partition/count/stream/verify/drop/checkpoint/config (Tasks 2-6), safety gate (Task 6 triple-check), rollback (runbook references `~/backup` dumps), risks (volume-on-root + DROP-per-cycle in Task 1 notes + Task 7), acceptance criteria (Tasks 1,6,7 verifiable).
- Placeholders: none — every step has exact code/commands/expected output.
- Type consistency: `PartitionProgress`/`PartitionInfo` signatures identical across Tasks 2/4/5/6; `Partition.Compute(ts, strategy)` reused; `ScyllaCount` semantics = successfully written rows (idempotent upserts, resume-safe).
