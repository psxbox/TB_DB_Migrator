using Npgsql;

namespace TbMigrator;

public class Orchestrator
{
    private readonly AppConfig       _cfg;
    private readonly PgReader        _reader;
    private readonly ScyllaWriter    _scylla;
    private readonly ProgressTracker _tracker;
    private readonly string          _connStr;
    private volatile bool            _stop;

    public Orchestrator(AppConfig cfg, PgReader reader, ScyllaWriter scylla, ProgressTracker tracker)
    {
        _cfg     = cfg;
        _reader  = reader;
        _scylla  = scylla;
        _tracker = tracker;
        _connStr = cfg.Pg.ConnectionString;
    }

    public async Task RunAsync(bool historicalOnly, bool resume, CancellationToken ct)
    {
        if (resume && _tracker.Load())
            Console.Error.WriteLine($"[INFO] Resuming: phase={_tracker.Progress.Phase}");
        else
            _tracker.Update(p =>
            {
                p.Phase        = "phase0";
                p.Partitioning = _cfg.Migrator.Partitioning;
                p.CastStrings  = _cfg.Migrator.CastStrings;
                p.StartedAt    = DateTimeOffset.UtcNow.ToString("O");
            });

        var (entityMap, keyMap, hybridMode) = await Phase0Async(ct);
        await Phase1Async(entityMap, keyMap, hybridMode, ct);

        if (!historicalOnly && !_stop)
            await Phase2Async(entityMap, keyMap, hybridMode, ct);
    }

    // -------------------------------------------------------------------------
    // Phase 0 — load maps
    // -------------------------------------------------------------------------
    private async Task<(Dictionary<string,string> entityMap,
                         Dictionary<int,string>    keyMap,
                         bool                      hybridMode)> Phase0Async(CancellationToken ct)
    {
        Console.Error.WriteLine("[INFO] Phase 0: loading entity map and key map");
        var entityMap            = await _reader.LoadEntityMapAsync(ct);
        var (keyMap, hybridMode) = await _reader.LoadKeyMapAsync(ct);
        var total                = await _reader.CountTsKvAsync(ct);
        Console.Error.WriteLine(
            $"[INFO] Phase 0 complete: {entityMap.Count} entities, {keyMap.Count} keys, {total:N0} ts_kv rows");

        _tracker.Update(p =>
        {
            p.Phase        = "phase1";
            p.Phase1StartTs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        });
        return (entityMap, keyMap, hybridMode);
    }

    // -------------------------------------------------------------------------
    // Phase 1 — historical migration with 2-level parallelism:
    //   Level 1: N entities concurrently
    //   Level 2: N key-chunks per entity concurrently
    // -------------------------------------------------------------------------
    private async Task Phase1Async(
        Dictionary<string, string> entityMap,
        Dictionary<int, string>    keyMap,
        bool                       hybridMode,
        CancellationToken          ct)
    {
        int workers = _cfg.Migrator.Workers;
        Console.Error.WriteLine($"[INFO] Phase 1: historical migration started (workers={workers})");

        var completed = _tracker.Progress.CompletedEntities.ToHashSet();
        var allEntities = await _reader.GetDistinctEntitiesAsync(ct);
        var pending = allEntities.Where(e => !completed.Contains(e)).ToList();
        Console.Error.WriteLine(
            $"[INFO] Phase 1: {allEntities.Count} entities total, {pending.Count} pending");

        var outerOpts = new ParallelOptions { MaxDegreeOfParallelism = workers, CancellationToken = ct };

        await Parallel.ForEachAsync(pending, outerOpts, async (entityId, ct2) =>
        {
            if (_stop) return;

            if (!entityMap.TryGetValue(entityId, out var entityType))
            {
                Console.Error.WriteLine($"[WARN] entity {entityId} not in entity_map, skipping");
                _tracker.Update(p => p.SkippedRows++);
                return;
            }

            await ProcessEntityAsync(entityId, entityType, keyMap, hybridMode, workers, ct2);

            _tracker.Update(p =>
            {
                p.LastEntityId = entityId;
                p.CompletedEntities.Add(entityId);
            });
        });

        // ts_kv_latest (single pass — table is small)
        Console.Error.WriteLine("[INFO] Phase 1: migrating ts_kv_latest");
        var latestByType = new Dictionary<string, List<TsRow>>();
        await foreach (var row in _reader.StreamTsKvLatestAsync(keyMap, hybridMode, ct))
        {
            if (!entityMap.TryGetValue(row.EntityId, out var et)) continue;
            if (!latestByType.ContainsKey(et)) latestByType[et] = [];
            latestByType[et].Add(row);
            if (latestByType[et].Count >= _cfg.Migrator.BatchSize)
            {
                await _scylla.WriteLatestBatchAsync(latestByType[et], et, _cfg.Migrator.CastStrings, ct);
                latestByType[et] = [];
            }
        }
        foreach (var (et, rows) in latestByType)
            if (rows.Count > 0)
                await _scylla.WriteLatestBatchAsync(rows, et, _cfg.Migrator.CastStrings, ct);

        Console.Error.WriteLine("[INFO] Phase 1 complete");
        _tracker.Update(p => p.Phase = "live_sync");
    }

    private async Task ProcessEntityAsync(
        string                  entityId,
        string                  entityType,
        Dictionary<int, string> keyMap,
        bool                    hybridMode,
        int                     workers,
        CancellationToken       ct)
    {
        // Get this entity's distinct keys from ts_kv_latest (fast index lookup)
        await using var keyConn = new NpgsqlConnection(_connStr);
        await keyConn.OpenAsync(ct);
        var keyReader = new PgReader(keyConn, hybridMode);
        var keys = await keyReader.GetEntityKeysAsync(entityId, ct);

        if (keys.Length == 0) return;

        // Split keys into worker-sized chunks for intra-entity parallelism
        int chunkSize = Math.Max(1, (int)Math.Ceiling((double)keys.Length / workers));
        var chunks    = keys.Chunk(chunkSize);

        var innerOpts = new ParallelOptions { MaxDegreeOfParallelism = workers, CancellationToken = ct };

        await Parallel.ForEachAsync(chunks, innerOpts, async (keyChunk, ct2) =>
        {
            if (_stop) return;
            await using var conn = new NpgsqlConnection(_connStr);
            await conn.OpenAsync(ct2);
            var chunkReader = new PgReader(conn, hybridMode);

            foreach (var key in keyChunk)
            {
                if (_stop) return;
                await foreach (var batch in chunkReader.StreamEntityKeyAsync(
                                   entityId, key, keyMap, hybridMode, _cfg.Migrator.BatchSize, ct2))
                {
                    var partitions = await _scylla.WriteTsBatchAsync(
                        batch, entityType, _cfg.Migrator.Partitioning, _cfg.Migrator.CastStrings, ct2);
                    await _scylla.WritePartitionsAsync(partitions, ct2);
                    _tracker.Update(p => p.MigratedRows += batch.Count);
                }
            }
        });
    }

    // -------------------------------------------------------------------------
    // Phase 2 — live sync
    // -------------------------------------------------------------------------
    private async Task Phase2Async(
        Dictionary<string, string> entityMap,
        Dictionary<int, string>    keyMap,
        bool                       hybridMode,
        CancellationToken          ct)
    {
        Console.Error.WriteLine("[INFO] Phase 2: live sync started");
        long watermark = _tracker.Progress.WatermarkTs != 0
            ? _tracker.Progress.WatermarkTs
            : _tracker.Progress.Phase1StartTs - 60_000;
        _tracker.Update(p => p.WatermarkTs = watermark);

        while (!_stop && !ct.IsCancellationRequested)
        {
            long nowMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
            var latestByType = new Dictionary<string, List<TsRow>>();
            var tsByType     = new Dictionary<string, List<TsRow>>();

            await foreach (var row in _reader.StreamTsKvByTsAsync(watermark, keyMap, hybridMode,
                               _cfg.Migrator.BatchSize, ct))
            {
                if (!entityMap.TryGetValue(row.EntityId, out var et)) continue;
                if (!tsByType.ContainsKey(et))     tsByType[et]     = [];
                if (!latestByType.ContainsKey(et)) latestByType[et] = [];
                tsByType[et].Add(row);
                latestByType[et].Add(row);
                if (row.Ts > watermark) watermark = row.Ts;
            }

            foreach (var (et, rows) in tsByType)
            {
                var partitions = await _scylla.WriteTsBatchAsync(
                    rows, et, _cfg.Migrator.Partitioning, _cfg.Migrator.CastStrings, ct);
                await _scylla.WritePartitionsAsync(partitions, ct);
            }
            foreach (var (et, rows) in latestByType)
                await _scylla.WriteLatestBatchAsync(rows, et, _cfg.Migrator.CastStrings, ct);

            long lag = nowMs - watermark;
            _tracker.Update(p => p.WatermarkTs = watermark);

            if (lag < _cfg.Migrator.LagThresholdMs)
                Console.Error.WriteLine($"[INFO] LAG {lag}ms < {_cfg.Migrator.LagThresholdMs}ms — ready for switchover");

            await Task.Delay(TimeSpan.FromSeconds(_cfg.Migrator.LiveSyncInterval), ct);
        }
    }

    public void RequestStop() => _stop = true;
}
