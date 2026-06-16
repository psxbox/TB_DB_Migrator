using YamlDotNet.Serialization;
using YamlDotNet.Serialization.NamingConventions;

namespace TbMigrator;

public class PgConfig
{
    public string Host     { get; set; } = "localhost";
    public int    Port     { get; set; } = 5432;
    public string Db       { get; set; } = "thingsboard";
    public string User     { get; set; } = "postgres";
    public string Password { get; set; } = "postgres";

    public string ConnectionString =>
        $"Host={Host};Port={Port};Database={Db};Username={User};Password={Password};" +
        "Pooling=false;CommandTimeout=600";
}

public class ScyllaConfig
{
    public string Host     { get; set; } = "localhost";
    public int    Port     { get; set; } = 9042;
    public string Keyspace { get; set; } = "thingsboard";
}

public class MigratorConfig
{
    [YamlMember(Alias = "batch_size")]
    public int    BatchSize        { get; set; } = 5000;

    public int    Workers          { get; set; } = 4;

    [YamlMember(Alias = "scylla_concurrency")]
    public int    ScyllaConcurrency { get; set; } = 128;

    [YamlMember(Alias = "live_sync_interval")]
    public double LiveSyncInterval  { get; set; } = 5.0;

    [YamlMember(Alias = "lag_threshold_ms")]
    public int    LagThresholdMs    { get; set; } = 30_000;

    public string Partitioning      { get; set; } = "MONTHS";

    [YamlMember(Alias = "cast_strings")]
    public bool   CastStrings       { get; set; } = false;

    [YamlMember(Alias = "checkpoint_file")]
    public string CheckpointFile    { get; set; } = "migration_progress.json";
}

public class AppConfig
{
    public PgConfig      Pg       { get; set; } = new();
    public ScyllaConfig  Scylla   { get; set; } = new();
    public MigratorConfig Migrator { get; set; } = new();

    public static AppConfig Load(string path = "config.yaml")
    {
        var d = new DeserializerBuilder()
            .WithNamingConvention(UnderscoredNamingConvention.Instance)
            .IgnoreUnmatchedProperties()
            .Build();

        AppConfig cfg = File.Exists(path)
            ? d.Deserialize<AppConfig>(File.ReadAllText(path)) ?? new()
            : new();

        // Env vars override config file
        cfg.Pg.Host     = Env("PG_HOST",     cfg.Pg.Host);
        cfg.Pg.Port     = int.Parse(Env("PG_PORT", cfg.Pg.Port.ToString()));
        cfg.Pg.Db       = Env("PG_DB",       cfg.Pg.Db);
        cfg.Pg.User     = Env("PG_USER",     cfg.Pg.User);
        cfg.Pg.Password = Env("PG_PASSWORD", cfg.Pg.Password);

        cfg.Scylla.Host     = Env("SCYLLA_HOST",     cfg.Scylla.Host);
        cfg.Scylla.Port     = int.Parse(Env("SCYLLA_PORT", cfg.Scylla.Port.ToString()));
        cfg.Scylla.Keyspace = Env("SCYLLA_KEYSPACE", cfg.Scylla.Keyspace);

        return cfg;
    }

    private static string Env(string key, string fallback) =>
        Environment.GetEnvironmentVariable(key) is { Length: > 0 } v ? v : fallback;
}
