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

    private static readonly string[] EntityTables =
    [
        "device", "customer", "tenant", "asset", "alarm", "dashboard",
        "rule_chain", "rule_node", "tb_user", "entity_view", "widgets_bundle",
        "widget_type", "tenant_profile", "device_profile", "api_usage_state",
        "edge", "ota_package", "rpc"
    ];

    private static readonly Dictionary<string, string> TableToType = new()
    {
        ["device"] = "DEVICE", ["customer"] = "CUSTOMER", ["tenant"] = "TENANT",
        ["asset"] = "ASSET", ["alarm"] = "ALARM", ["dashboard"] = "DASHBOARD",
        ["rule_chain"] = "RULE_CHAIN", ["rule_node"] = "RULE_NODE",
        ["tb_user"] = "USER", ["entity_view"] = "ENTITY_VIEW",
        ["widgets_bundle"] = "WIDGETS_BUNDLE", ["widget_type"] = "WIDGET_TYPE",
        ["tenant_profile"] = "TENANT_PROFILE", ["device_profile"] = "DEVICE_PROFILE",
        ["api_usage_state"] = "API_USAGE_STATE", ["edge"] = "EDGE",
        ["ota_package"] = "OTA_PACKAGE", ["rpc"] = "RPC"
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

    public async Task<(Dictionary<int, string> Map, bool Hybrid)> LoadKeyMapAsync(CancellationToken ct = default)
    {
        foreach (var table in new[] { "key_dictionary", "ts_kv_dictionary" })
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

    public async IAsyncEnumerable<TsRow> StreamTsKvByTsAsync(
        long watermarkTs,
        Dictionary<int, string> keyMap,
        bool hybridMode,
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
                var row = ReadRow(rdr, keyMap, hybridMode);
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

    // --- Helpers ------------------------------------------------------------

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
