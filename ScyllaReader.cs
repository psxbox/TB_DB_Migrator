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
