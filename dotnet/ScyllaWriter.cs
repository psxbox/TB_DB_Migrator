using Cassandra;

namespace TbMigrator;

public record PartitionKey(string EntityType, Guid EntityId, string Key, long Partition);

public class ScyllaWriter
{
    private readonly ISession _session;
    private readonly string   _keyspace;
    private readonly SemaphoreSlim _writeSem;
    private readonly SemaphoreSlim _prepareSem = new(1, 1);

    private PreparedStatement? _psTs;
    private PreparedStatement? _psPartition;
    private PreparedStatement? _psLatest;

    private static readonly string[] Schema =
    [
        "CREATE KEYSPACE IF NOT EXISTS {0} WITH replication = {{'class':'SimpleStrategy','replication_factor':1}}",
        "CREATE TABLE IF NOT EXISTS {0}.ts_kv_cf (" +
            "entity_type text, entity_id timeuuid, key text, partition bigint, ts bigint," +
            "bool_v boolean, str_v text, long_v bigint, dbl_v double, json_v text," +
            "PRIMARY KEY ((entity_type,entity_id,key,partition),ts))",
        "CREATE TABLE IF NOT EXISTS {0}.ts_kv_partitions_cf (" +
            "entity_type text, entity_id timeuuid, key text, partition bigint," +
            "PRIMARY KEY ((entity_type,entity_id,key),partition))" +
            " WITH CLUSTERING ORDER BY (partition ASC)" +
            " AND compaction={'class':'LeveledCompactionStrategy'}",
        "CREATE TABLE IF NOT EXISTS {0}.ts_kv_latest_cf (" +
            "entity_type text, entity_id timeuuid, key text," +
            "ts bigint, bool_v boolean, str_v text, long_v bigint, dbl_v double, json_v text," +
            "PRIMARY KEY ((entity_type,entity_id),key))" +
            " WITH compaction={'class':'LeveledCompactionStrategy'}",
    ];

    public static ScyllaWriter Connect(string host, int port, string keyspace, int concurrency)
    {
        var cluster = Cluster.Builder()
            .AddContactPoint(host)
            .WithPort(port)
            .WithCompression(CompressionType.NoCompression)
            .Build();
        var session = cluster.Connect();
        return new ScyllaWriter(session, keyspace, concurrency);
    }

    private ScyllaWriter(ISession session, string keyspace, int concurrency)
    {
        _session  = session;
        _keyspace = keyspace;
        _writeSem = new SemaphoreSlim(concurrency, concurrency);
    }

    public void InitSchema()
    {
        foreach (var tpl in Schema)
            _session.Execute(string.Format(tpl, _keyspace));
    }

    private async Task EnsurePreparedAsync()
    {
        if (_psTs is not null) return;
        await _prepareSem.WaitAsync();
        try
        {
            if (_psTs is not null) return;
            var ks = _keyspace;
            _psTs = await _session.PrepareAsync(
                $"INSERT INTO {ks}.ts_kv_cf " +
                "(entity_type,entity_id,key,partition,ts,bool_v,str_v,long_v,dbl_v,json_v) " +
                "VALUES (?,?,?,?,?,?,?,?,?,?)");
            _psPartition = await _session.PrepareAsync(
                $"INSERT INTO {ks}.ts_kv_partitions_cf " +
                "(entity_type,entity_id,key,partition) VALUES (?,?,?,?)");
            _psLatest = await _session.PrepareAsync(
                $"INSERT INTO {ks}.ts_kv_latest_cf " +
                "(entity_type,entity_id,key,ts,bool_v,str_v,long_v,dbl_v,json_v) " +
                "VALUES (?,?,?,?,?,?,?,?,?)");
        }
        finally { _prepareSem.Release(); }
    }

    public async Task<HashSet<PartitionKey>> WriteTsBatchAsync(
        List<TsRow> rows,
        string entityType,
        string partitioning,
        bool castStrings,
        CancellationToken ct = default)
    {
        if (rows.Count == 0) return [];
        await EnsurePreparedAsync();

        var partitions = new HashSet<PartitionKey>();
        var tasks = new List<Task>(rows.Count);

        foreach (var row in rows)
        {
            var effective = castStrings ? TryCast(row) : row;
            var eid       = Guid.Parse(row.EntityId);
            var partition = Partition.Compute(row.Ts, partitioning);
            partitions.Add(new PartitionKey(entityType, eid, row.Key, partition));

            var bound = _psTs!.Bind(
                entityType, eid, row.Key, partition, row.Ts,
                (object?)effective.BoolV,
                (object?)effective.StrV,
                (object?)effective.LongV,
                (object?)effective.DblV,
                (object?)effective.JsonV);

            tasks.Add(ExecuteWithSemAsync(bound, ct));
        }
        await Task.WhenAll(tasks);
        return partitions;
    }

    public async Task WritePartitionsAsync(HashSet<PartitionKey> partitions, CancellationToken ct = default)
    {
        if (partitions.Count == 0) return;
        await EnsurePreparedAsync();
        var tasks = partitions.Select(p =>
            ExecuteWithSemAsync(_psPartition!.Bind(p.EntityType, p.EntityId, p.Key, p.Partition), ct));
        await Task.WhenAll(tasks);
    }

    public async Task WriteLatestBatchAsync(
        List<TsRow> rows,
        string entityType,
        bool castStrings,
        CancellationToken ct = default)
    {
        if (rows.Count == 0) return;
        await EnsurePreparedAsync();
        var tasks = rows.Select(row =>
        {
            var effective = castStrings ? TryCast(row) : row;
            var eid = Guid.Parse(row.EntityId);
            var bound = _psLatest!.Bind(
                entityType, eid, row.Key, row.Ts,
                (object?)effective.BoolV,
                (object?)effective.StrV,
                (object?)effective.LongV,
                (object?)effective.DblV,
                (object?)effective.JsonV);
            return ExecuteWithSemAsync(bound, ct);
        });
        await Task.WhenAll(tasks);
    }

    private async Task ExecuteWithSemAsync(IStatement stmt, CancellationToken ct)
    {
        await _writeSem.WaitAsync(ct);
        try   { await _session.ExecuteAsync(stmt); }
        finally { _writeSem.Release(); }
    }

    private static TsRow TryCast(TsRow row)
    {
        if (row.StrV is null) return row;
        if (long.TryParse(row.StrV, out var l))
            return row with { StrV = null, LongV = l };
        if (double.TryParse(row.StrV, System.Globalization.NumberStyles.Float,
                System.Globalization.CultureInfo.InvariantCulture, out var d))
            return row with { StrV = null, DblV = d };
        return row;
    }
}
