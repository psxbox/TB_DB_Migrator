using Npgsql;

namespace TbMigrator;

public record TsRow(
    string  EntityId,
    string  Key,        // already resolved to key_name
    long    Ts,
    bool?   BoolV,
    string? StrV,
    long?   LongV,
    double? DblV,
    string? JsonV);

public record PartitionInfo(string Name, long MinTs, long MaxTs, long Count, long SizeBytes);

public class PgReader : IAsyncDisposable
{
    private readonly NpgsqlConnection _conn;
    private readonly bool _hybridMode; // true = key column is integer in ts_kv

    public PgReader(NpgsqlConnection conn, bool hybridMode)
    {
        _conn = conn;
        _hybridMode = hybridMode;
    }

    public static async Task<PgReader> ConnectAsync(string connectionString, CancellationToken ct = default)
    {
        var conn = new NpgsqlConnection(connectionString);
        await conn.OpenAsync(ct);
        return new PgReader(conn, hybridMode: false); // hybridMode set after LoadKeyMap
    }

    // --- Entity map ---------------------------------------------------------
    // TB 3.4 (official migrator RelatedEntitiesParser) entity tables.
    // ts_kv.ts_kv_dictionary based hybrid mode: ts_kv.key is integer FK.

    private static readonly string[] EntityTables =
    [
        "device", "customer", "tenant", "asset", "alarm", "dashboard",
        "rule_chain", "rule_node", "tb_user", "entity_view", "widgets_bundle",
        "widget_type", "tenant_profile", "device_profile", "asset_profile",
        "api_usage_state", "edge", "ota_package", "rpc"
    ];

    private static readonly Dictionary<string, string> TableToType = new()
    {
        ["device"] = "DEVICE", ["customer"] = "CUSTOMER", ["tenant"] = "TENANT",
        ["asset"] = "ASSET", ["alarm"] = "ALARM", ["dashboard"] = "DASHBOARD",
        ["rule_chain"] = "RULE_CHAIN", ["rule_node"] = "RULE_NODE",
        ["tb_user"] = "USER", ["entity_view"] = "ENTITY_VIEW",
        ["widgets_bundle"] = "WIDGETS_BUNDLE", ["widget_type"] = "WIDGET_TYPE",
        ["tenant_profile"] = "TENANT_PROFILE", ["device_profile"] = "DEVICE_PROFILE",
        ["asset_profile"] = "ASSET_PROFILE",
        ["api_usage_state"] = "API_USAGE_STATE",
        ["edge"] = "EDGE", ["ota_package"] = "OTA_PACKAGE", ["rpc"] = "RPC"
    };

    public async Task<Dictionary<string, string>> LoadEntityMapAsync(CancellationToken ct = default)
    {
        var map = new Dictionary<string, string>();
        foreach (var table in EntityTables)
        {
            try
            {
                await using var cmd = _conn.CreateCommand();
                cmd.CommandText = $"SELECT id FROM {table}";
                await using var rdr = await cmd.ExecuteReaderAsync(ct);
                while (await rdr.ReadAsync(ct))
                    map[rdr.GetGuid(0).ToString()] = TableToType[table];
            }
            catch { /* table missing — skip */ }
        }
        return map;
    }

    // --- Key map ------------------------------------------------------------
    // TB 3.4: ts_kv_dictionary (key varchar PK, key_id serial UNIQUE).
    // ts_kv.key + ts_kv_latest.key are integer FK -> key_id.
    // TB 4.x renamed it to key_dictionary — try 3.4 name first.

    public async Task<(Dictionary<int, string> Map, bool Hybrid)> LoadKeyMapAsync(CancellationToken ct = default)
    {
        foreach (var table in new[] { "ts_kv_dictionary", "key_dictionary" })
        {
            try
            {
                await using var cmd = _conn.CreateCommand();
                cmd.CommandText = $"SELECT key_id, key FROM {table}";
                await using var rdr = await cmd.ExecuteReaderAsync(ct);
                var map = new Dictionary<int, string>();
                while (await rdr.ReadAsync(ct))
                    map[rdr.GetInt32(0)] = rdr.GetString(1);
                Console.Error.WriteLine($"[INFO] Loaded {map.Count} keys from {table}");
                return (map, Hybrid: true);
            }
            catch { /* try next */ }
        }
        Console.Error.WriteLine("[INFO] No key dictionary — pure-SQL mode");
        return ([], Hybrid: false);
    }

    // --- Distinct entities --------------------------------------------------

    public async Task<List<string>> GetDistinctEntitiesAsync(CancellationToken ct = default)
    {
        // Prefer ts_kv_latest (much smaller than ts_kv)
        string source = await HasRowsAsync("ts_kv_latest", ct) ? "ts_kv_latest" : "ts_kv";
        Console.Error.WriteLine($"[INFO] Reading distinct entities from {source}");

        await using var cmd = _conn.CreateCommand();
        cmd.CommandText = $"SELECT DISTINCT entity_id FROM {source}";
        await using var rdr = await cmd.ExecuteReaderAsync(ct);
        var list = new List<string>();
        while (await rdr.ReadAsync(ct))
            list.Add(rdr.GetGuid(0).ToString());
        return list;
    }

    private async Task<bool> HasRowsAsync(string table, CancellationToken ct)
    {
        try
        {
            await using var cmd = _conn.CreateCommand();
            cmd.CommandText = $"SELECT 1 FROM {table} LIMIT 1";
            return await cmd.ExecuteScalarAsync(ct) is not null;
        }
        catch { return false; }
    }

    // --- Entity key list (for intra-entity parallelism) ---------------------

    public async Task<string[]> GetEntityKeysAsync(string entityId, CancellationToken ct = default)
    {
        await using var cmd = _conn.CreateCommand();
        cmd.CommandText = "SELECT DISTINCT key::text FROM ts_kv_latest WHERE entity_id = $1";
        cmd.Parameters.AddWithValue(Guid.Parse(entityId));
        await using var rdr = await cmd.ExecuteReaderAsync(ct);
        var keys = new List<string>();
        while (await rdr.ReadAsync(ct))
            keys.Add(rdr.GetString(0));
        return keys.ToArray();
    }

    // --- ts_kv streaming per entity-key (keyset pagination) -----------------

    public async IAsyncEnumerable<List<TsRow>> StreamEntityKeyAsync(
        string entityId,
        string key,
        Dictionary<int, string> keyMap,
        bool hybridMode,
        int batchSize,
        [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken ct = default)
    {
        long lastTs = long.MinValue;
        var eid = Guid.Parse(entityId);
        string keyName = ResolveKeyName(key, keyMap, hybridMode);

        while (true)
        {
            await using var cmd = _conn.CreateCommand();
            if (hybridMode)
            {
                cmd.CommandText =
                    "SELECT entity_id, ts, bool_v, str_v, long_v, dbl_v, json_v " +
                    "FROM ts_kv WHERE entity_id = $1 AND key = $2 AND ts > $3 " +
                    "ORDER BY ts ASC LIMIT $4";
                cmd.Parameters.AddWithValue(eid);
                cmd.Parameters.AddWithValue(int.Parse(key));
                cmd.Parameters.AddWithValue(lastTs);
                cmd.Parameters.AddWithValue(batchSize);
            }
            else
            {
                cmd.CommandText =
                    "SELECT entity_id, ts, bool_v, str_v, long_v, dbl_v, json_v " +
                    "FROM ts_kv WHERE entity_id = $1 AND key = $2 AND ts > $3 " +
                    "ORDER BY ts ASC LIMIT $4";
                cmd.Parameters.AddWithValue(eid);
                cmd.Parameters.AddWithValue(key);
                cmd.Parameters.AddWithValue(lastTs);
                cmd.Parameters.AddWithValue(batchSize);
            }

            var batch = new List<TsRow>(batchSize);
            await using var rdr = await cmd.ExecuteReaderAsync(ct);
            while (await rdr.ReadAsync(ct))
            {
                batch.Add(new TsRow(
                    EntityId: entityId,
                    Key:      keyName,
                    Ts:       rdr.GetInt64(1),
                    BoolV:    rdr.IsDBNull(2) ? null : rdr.GetBoolean(2),
                    StrV:     rdr.IsDBNull(3) ? null : rdr.GetString(3),
                    LongV:    rdr.IsDBNull(4) ? null : rdr.GetInt64(4),
                    DblV:     rdr.IsDBNull(5) ? null : rdr.GetDouble(5),
                    JsonV:    rdr.IsDBNull(6) ? null : rdr.GetString(6)));
                lastTs = batch[^1].Ts;
            }

            if (batch.Count > 0) yield return batch;
            if (batch.Count < batchSize) yield break;
        }
    }

    // --- ts_kv_latest streaming ---------------------------------------------

    public async IAsyncEnumerable<TsRow> StreamTsKvLatestAsync(
        Dictionary<int, string> keyMap,
        bool hybridMode,
        [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken ct = default)
    {
        await using var cmd = _conn.CreateCommand();
        cmd.CommandText =
            "SELECT entity_id, key::text, ts, bool_v, str_v, long_v, dbl_v, json_v FROM ts_kv_latest";
        await using var rdr = await cmd.ExecuteReaderAsync(ct);
        while (await rdr.ReadAsync(ct))
            yield return ReadRow(rdr, keyMap, hybridMode);
    }

    // --- Live sync (ts > watermark) -----------------------------------------
    // TB 3.4: ts_kv.key is integer — numeric comparison must stay numeric.
    // Comparing key::text breaks ordering ('10' < '2'), so hybrid and
    // pure-SQL modes use separate queries.

    public async IAsyncEnumerable<TsRow> StreamTsKvByTsAsync(
        long watermarkTs,
        Dictionary<int, string> keyMap,
        bool hybridMode,
        int batchSize,
        [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken ct = default)
    {
        if (hybridMode)
            await foreach (var row in StreamTsKvByTsHybridAsync(watermarkTs, keyMap, batchSize, ct))
                yield return row;
        else
            await foreach (var row in StreamTsKvByTsTextAsync(watermarkTs, keyMap, batchSize, ct))
                yield return row;
    }

    private async IAsyncEnumerable<TsRow> StreamTsKvByTsHybridAsync(
        long watermarkTs,
        Dictionary<int, string> keyMap,
        int batchSize,
        [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken ct = default)
    {
        (long, Guid, int)? last = null;
        while (true)
        {
            await using var cmd = _conn.CreateCommand();
            if (last is null)
            {
                cmd.CommandText =
                    "SELECT entity_id, key, ts, bool_v, str_v, long_v, dbl_v, json_v " +
                    "FROM ts_kv WHERE ts > $1 ORDER BY ts, entity_id, key LIMIT $2";
                cmd.Parameters.AddWithValue(watermarkTs);
                cmd.Parameters.AddWithValue(batchSize);
            }
            else
            {
                cmd.CommandText =
                    "SELECT entity_id, key, ts, bool_v, str_v, long_v, dbl_v, json_v " +
                    "FROM ts_kv WHERE (ts, entity_id, key) > ($1,$2,$3) " +
                    "ORDER BY ts, entity_id, key LIMIT $4";
                cmd.Parameters.AddWithValue(last.Value.Item1);
                cmd.Parameters.AddWithValue(last.Value.Item2);
                cmd.Parameters.AddWithValue(last.Value.Item3);
                cmd.Parameters.AddWithValue(batchSize);
            }

            int count = 0;
            int lastKey = 0;
            Guid lastEid = Guid.Empty;
            long lastTs = 0;
            await using var rdr = await cmd.ExecuteReaderAsync(ct);
            while (await rdr.ReadAsync(ct))
            {
                var row = ReadRowIntKey(rdr, keyMap);
                lastEid = Guid.Parse(row.EntityId);
                lastKey = rdr.GetInt32(1);
                lastTs  = row.Ts;
                count++;
                yield return row;
            }
            if (count == 0) yield break;
            last = (lastTs, lastEid, lastKey);
            if (count < batchSize) yield break;
        }
    }

    private async IAsyncEnumerable<TsRow> StreamTsKvByTsTextAsync(
        long watermarkTs,
        Dictionary<int, string> keyMap,
        int batchSize,
        [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken ct = default)
    {
        (long, Guid, string)? last = null;
        while (true)
        {
            await using var cmd = _conn.CreateCommand();
            if (last is null)
            {
                cmd.CommandText =
                    "SELECT entity_id, key::text, ts, bool_v, str_v, long_v, dbl_v, json_v " +
                    "FROM ts_kv WHERE ts > $1 ORDER BY ts, entity_id, key LIMIT $2";
                cmd.Parameters.AddWithValue(watermarkTs);
                cmd.Parameters.AddWithValue(batchSize);
            }
            else
            {
                cmd.CommandText =
                    "SELECT entity_id, key::text, ts, bool_v, str_v, long_v, dbl_v, json_v " +
                    "FROM ts_kv WHERE (ts, entity_id, key::text) > ($1,$2,$3) " +
                    "ORDER BY ts, entity_id, key LIMIT $4";
                cmd.Parameters.AddWithValue(last.Value.Item1);
                cmd.Parameters.AddWithValue(last.Value.Item2);
                cmd.Parameters.AddWithValue(last.Value.Item3);
                cmd.Parameters.AddWithValue(batchSize);
            }

            int count = 0;
            string lastKey = "";
            Guid lastEid = Guid.Empty;
            long lastTs = 0;
            await using var rdr = await cmd.ExecuteReaderAsync(ct);
            while (await rdr.ReadAsync(ct))
            {
                var row = ReadRow(rdr, keyMap, hybridMode: false);
                lastEid = Guid.Parse(row.EntityId);
                lastKey = rdr.GetString(1); // raw key (before name resolution) for pagination
                lastTs  = row.Ts;
                count++;
                yield return row;
            }
            if (count == 0) yield break;
            last = (lastTs, lastEid, lastKey);
            if (count < batchSize) yield break;
        }
    }

    // --- Row count ----------------------------------------------------------

    public async Task<long> CountTsKvAsync(CancellationToken ct = default)
    {
        await using var cmd = _conn.CreateCommand();
        cmd.CommandText = "SELECT COUNT(*) FROM ts_kv";
        return (long)(await cmd.ExecuteScalarAsync(ct))!;
    }

    // --- Partitions ---------------------------------------------------------

    // Name-only listing — catalog query, instant. Use for --partition allowlist
    // validation; ListPartitionsAsync also computes per-partition stats
    // (COUNT/MIN/MAX full scans on every child) which takes minutes on large sets.
    public async Task<List<string>> ListPartitionNamesAsync(CancellationToken ct = default)
    {
        var names = new List<string>();
        await using var cmd = _conn.CreateCommand();
        cmd.CommandText = "SELECT inhrelid::regclass::text FROM pg_inherits WHERE inhparent = 'ts_kv'::regclass ORDER BY 1";
        await using var rdr = await cmd.ExecuteReaderAsync(ct);
        while (await rdr.ReadAsync(ct))
            names.Add(rdr.GetString(0));
        return names;
    }

    public async Task<List<PartitionInfo>> ListPartitionsAsync(CancellationToken ct = default)
    {
        var names = await ListPartitionNamesAsync(ct);
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
                var row = hybridMode ? ReadRowIntKey(rdr, keyMap) : ReadRow(rdr, keyMap, false);
                batch.Add(row);
                last = (row.Ts, Guid.Parse(row.EntityId), rdr[1].ToString()!);
            }
            if (batch.Count > 0) yield return batch;
            if (batch.Count < batchSize) yield break;
        }
    }

    // --- Partition streaming in PRIMARY KEY order ----------------------------
    // ORDER BY (entity_id, key, ts) — the child partition's PK index serves the
    // scan directly (O(N)). The ts-ordered variant above is O(N²): every page
    // full-scans + sorts the whole partition, because there is no index on ts.
    public async IAsyncEnumerable<List<TsRow>> StreamPartitionPkAsync(
        string partition,
        (Guid EntityId, string KeyRaw, long Ts)? resumeCursor,
        Dictionary<int, string> keyMap,
        bool hybridMode,
        int batchSize,
        [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken ct = default)
    {
        var last = resumeCursor;
        while (true)
        {
            await using var cmd = _conn.CreateCommand();
            string keySel = hybridMode ? "key" : "key::text";
            if (last is null)
            {
                cmd.CommandText =
                    $"SELECT entity_id, {keySel}, ts, bool_v, str_v, long_v, dbl_v, json_v " +
                    $"FROM \"{partition}\" ORDER BY entity_id, key, ts LIMIT $1";
                cmd.Parameters.AddWithValue(batchSize);
            }
            else
            {
                cmd.CommandText =
                    $"SELECT entity_id, {keySel}, ts, bool_v, str_v, long_v, dbl_v, json_v " +
                    $"FROM \"{partition}\" WHERE (entity_id, key, ts) > ($1,$2,$3) " +
                    "ORDER BY entity_id, key, ts LIMIT $4";
                cmd.Parameters.AddWithValue(last.Value.EntityId);
                // hybrid: key column is int — parse back; text mode: raw string
                if (hybridMode && int.TryParse(last.Value.KeyRaw, out int kid))
                    cmd.Parameters.AddWithValue(kid);
                else
                    cmd.Parameters.AddWithValue(last.Value.KeyRaw);
                cmd.Parameters.AddWithValue(last.Value.Ts);
                cmd.Parameters.AddWithValue(batchSize);
            }

            var batch = new List<TsRow>(batchSize);
            await using var rdr = await cmd.ExecuteReaderAsync(ct);
            while (await rdr.ReadAsync(ct))
            {
                var row = hybridMode ? ReadRowIntKey(rdr, keyMap) : ReadRow(rdr, keyMap, false);
                batch.Add(row);
                last = (Guid.Parse(row.EntityId), rdr[1].ToString()!, row.Ts);
            }
            if (batch.Count > 0) yield return batch;
            if (batch.Count < batchSize) yield break;
        }
    }

    // --- Helpers ------------------------------------------------------------

    private static TsRow ReadRowIntKey(NpgsqlDataReader rdr, Dictionary<int, string> keyMap)
    {
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

    private static TsRow ReadRow(NpgsqlDataReader rdr, Dictionary<int, string> keyMap, bool hybridMode)
    {
        var rawKey = rdr.GetString(1);
        string keyName = ResolveKeyName(rawKey, keyMap, hybridMode);
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

    private static string ResolveKeyName(string rawKey, Dictionary<int, string> keyMap, bool hybridMode)
    {
        if (!hybridMode) return rawKey;
        return int.TryParse(rawKey, out int id) && keyMap.TryGetValue(id, out var name)
            ? name
            : rawKey;
    }

    public async ValueTask DisposeAsync() => await _conn.DisposeAsync();
}
