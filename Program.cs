using Npgsql;
using Spectre.Console;

namespace TbMigrator;

internal static class Program
{
    static async Task<int> Main(string[] args)
    {
        string? command = args.Length > 0 ? args[0] : null;
        string  config  = Flag(args, "--config") ?? "config.yaml";

        switch (command)
        {
            case "init-schema": return await RunInitSchema(config);
            case "start":       return await RunStart(args, config);
            case "status":      return RunStatus(config);
            case "list-partitions": return await RunListPartitions(config);
            case "verify":          return await RunVerify(args, config);
            case "drop":            return await RunDrop(args, config);
            default:
                Console.Error.WriteLine("Usage: tbmigrator <init-schema|start|status|list-partitions|verify|drop> [--config config.yaml]");
                Console.Error.WriteLine("  start [--partition PART] [--delta-from TSMS] [--resume] [--workers N] [--historical-only] [--config FILE]");
                Console.Error.WriteLine("  list-partitions [--config FILE]");
                Console.Error.WriteLine("  verify --partition PART [--config FILE]");
                Console.Error.WriteLine("  drop --partition PART --dump-file PATH --verified [--config FILE]");
                return 1;
        }
    }

    // -------------------------------------------------------------------------
    static async Task<int> RunInitSchema(string cfgPath)
    {
        var cfg   = AppConfig.Load(cfgPath);
        var scylla = ScyllaWriter.Connect(
            cfg.Scylla.Host, cfg.Scylla.Port,
            cfg.Scylla.Keyspace, cfg.Migrator.ScyllaConcurrency);
        scylla.InitSchema();
        AnsiConsole.MarkupLine("[green]Schema initialised.[/]");
        return 0;
    }

    // -------------------------------------------------------------------------
    static async Task<int> RunStart(string[] args, string cfgPath)
    {
        var cfg           = AppConfig.Load(cfgPath);
        bool resume       = HasFlag(args, "--resume");
        bool histOnly     = HasFlag(args, "--historical-only");
        string? wStr      = Flag(args, "--workers");
        if (wStr is not null && int.TryParse(wStr, out int w) && w > 0)
            cfg.Migrator.Workers = w;
        string? part = Flag(args, "--partition");
        long deltaFrom = long.TryParse(Flag(args, "--delta-from"), out var d) ? d : long.MinValue;

        AnsiConsole.MarkupLine(
            $"[bold]TB Migrator (.NET 10)[/]  workers=[yellow]{cfg.Migrator.Workers}[/]  " +
            $"partitioning=[yellow]{cfg.Migrator.Partitioning}[/]  " +
            $"scylla_concurrency=[yellow]{cfg.Migrator.ScyllaConcurrency}[/]");

        using var cts = new CancellationTokenSource();
        Console.CancelKeyPress += (_, e) => { e.Cancel = true; cts.Cancel(); };
        AppDomain.CurrentDomain.ProcessExit += (_, _) => cts.Cancel();

        await using var conn = new NpgsqlConnection(cfg.Pg.ConnectionString);
        await conn.OpenAsync(cts.Token);

        var (keyMap, hybridMode) = await new PgReader(conn, false).LoadKeyMapAsync(cts.Token);
        var reader  = new PgReader(conn, hybridMode);
        if (part is not null)
        {
            var names = await reader.ListPartitionsAsync(cts.Token);
            if (!names.Any(p => p.Name == part))
            {
                Console.Error.WriteLine($"Unknown partition '{part}'. Use list-partitions to see valid names.");
                return 1;
            }
        }
        var scylla  = ScyllaWriter.Connect(
            cfg.Scylla.Host, cfg.Scylla.Port,
            cfg.Scylla.Keyspace, cfg.Migrator.ScyllaConcurrency);

        scylla.InitSchema();

        var tracker = new ProgressTracker(cfg.Migrator.CheckpointFile);
        var orch    = new Orchestrator(cfg, reader, scylla, tracker);

        _ = Task.Run(() => PrintLoop(tracker, cts.Token));

        try
        {
            if (part is not null)
            {
                tracker.Load();
                if (resume && deltaFrom == long.MinValue &&
                    tracker.Progress.Partitions.TryGetValue(part, out var prev))
                    Console.Error.WriteLine(
                        $"[INFO] Resuming partition {part}: stored scylla_count={prev.ScyllaCount} max_ts={prev.MaxTs} " +
                        $"(Scylla INSERTs are idempotent upserts — resume double-write is harmless). " +
                        $"Pass --delta-from {prev.MaxTs} to skip already-streamed rows.");
                await orch.RunPartitionAsync(part, deltaFrom, cfg.Migrator.Workers, cts.Token);
                AnsiConsole.MarkupLine($"[green]Partition {part} migration complete.[/]");
                return 0;
            }
            await orch.RunAsync(histOnly, resume, cts.Token);
            AnsiConsole.MarkupLine("[green]Migration complete.[/]");
            return 0;
        }
        catch (OperationCanceledException)
        {
            AnsiConsole.MarkupLine("[yellow]Interrupted — checkpoint saved.[/]");
            return 0;
        }
    }

    // -------------------------------------------------------------------------
    static async Task<int> RunListPartitions(string cfgPath)
    {
        var cfg = AppConfig.Load(cfgPath);
        await using var conn = new NpgsqlConnection(cfg.Pg.ConnectionString);
        await conn.OpenAsync();
        var reader = new PgReader(conn, false);
        var parts = await reader.ListPartitionsAsync();

        var table = new Table();
        table.AddColumn("Name");
        table.AddColumn("MinTs");
        table.AddColumn("MaxTs");
        table.AddColumn("Count");
        table.AddColumn("Size MB");
        foreach (var p in parts)
        {
            table.AddRow(
                p.Name,
                DateTimeOffset.FromUnixTimeMilliseconds(p.MinTs).ToString("O"),
                DateTimeOffset.FromUnixTimeMilliseconds(p.MaxTs).ToString("O"),
                $"{p.Count:N0}",
                $"{p.SizeBytes / 1024.0 / 1024.0:F1}");
        }
        AnsiConsole.Write(table);
        return 0;
    }

    // -------------------------------------------------------------------------
    static async Task<int> RunVerify(string[] args, string cfgPath)
    {
        string? part = Flag(args, "--partition");
        if (part is null)
        {
            Console.Error.WriteLine("Missing required --partition <name>.");
            return 1;
        }

        var cfg     = AppConfig.Load(cfgPath);
        var tracker = new ProgressTracker(cfg.Migrator.CheckpointFile);
        if (!tracker.Load() || !tracker.Progress.Partitions.TryGetValue(part, out var entry))
        {
            Console.Error.WriteLine($"No checkpoint entry for partition '{part}'. Migrate it first.");
            return 1;
        }
        if (entry.State != "migrated")
        {
            Console.Error.WriteLine(
                $"Partition '{part}' state is '{entry.State}' — verify only runs on migrated partitions.");
            return 1;
        }

        await using var conn = new NpgsqlConnection(cfg.Pg.ConnectionString);
        await conn.OpenAsync();
        var reader = new PgReader(conn, false);

        // Validate --partition against server list before use in any SQL.
        var names = await reader.ListPartitionsAsync();
        if (!names.Any(p => p.Name == part))
        {
            Console.Error.WriteLine($"Unknown partition '{part}'. Use list-partitions to see valid names.");
            return 1;
        }

        long pgCount = await reader.CountPartitionAsync(part);
        if (pgCount != entry.ScyllaCount)
        {
            Console.Error.WriteLine(
                $"Count mismatch for '{part}': pg={pgCount:N0} scylla={entry.ScyllaCount:N0}.");
            return 1;
        }

        var (keyMap, hybridMode) = await reader.LoadKeyMapAsync();
        var entityMap = await reader.LoadEntityMapAsync();

        int n = cfg.Migrator.VerifySampleSize;
        var sample = new List<TsRow>(n);
        await using (var cmd = conn.CreateCommand())
        {
            // Validated partition name only — identifiers cannot be parameterised.
            cmd.CommandText =
                $"SELECT entity_id, key::text, ts, bool_v, str_v, long_v, dbl_v, json_v " +
                $"FROM \"{part}\" ORDER BY random() LIMIT {n}";
            await using var rdr = await cmd.ExecuteReaderAsync();
            while (await rdr.ReadAsync())
            {
                string rawKey = rdr.GetString(1);
                string keyName = hybridMode
                    && int.TryParse(rawKey, out int kid)
                    && keyMap.TryGetValue(kid, out var kn)
                    ? kn : rawKey;
                sample.Add(new TsRow(
                    EntityId: rdr.GetGuid(0).ToString(),
                    Key:      keyName,
                    Ts:       rdr.GetInt64(2),
                    BoolV:    rdr.IsDBNull(3) ? null : rdr.GetBoolean(3),
                    StrV:     rdr.IsDBNull(4) ? null : rdr.GetString(4),
                    LongV:    rdr.IsDBNull(5) ? null : rdr.GetInt64(5),
                    DblV:     rdr.IsDBNull(6) ? null : rdr.GetDouble(6),
                    JsonV:    rdr.IsDBNull(7) ? null : rdr.GetString(7)));
            }
        }

        var scylla  = ScyllaWriter.Connect(
            cfg.Scylla.Host, cfg.Scylla.Port,
            cfg.Scylla.Keyspace, cfg.Migrator.ScyllaConcurrency);
        var sreader = new ScyllaReader(scylla.Session, cfg.Scylla.Keyspace);

        int mismatches = 0;
        foreach (var row in sample)
        {
            if (!entityMap.TryGetValue(row.EntityId, out var et)) { mismatches++; continue; }
            var got = await sreader.GetPointAsync(
                et, Guid.Parse(row.EntityId), row.Key,
                Partition.Compute(row.Ts, cfg.Migrator.Partitioning), row.Ts);
            if (got is null
                || got.Ts    != row.Ts
                || got.BoolV != row.BoolV
                || got.StrV  != row.StrV
                || got.LongV != row.LongV
                || got.DblV  != row.DblV
                || got.JsonV != row.JsonV)
                mismatches++;
        }

        if (mismatches == 0)
        {
            tracker.Update(p => p.Partitions[part] = p.Partitions[part] with { Verified = true, State = "verified" });
            AnsiConsole.MarkupLine(
                $"[green]Verify OK for '{part}': pg={pgCount:N0} scylla={entry.ScyllaCount:N0} " +
                $"samples={sample.Count} mismatches=0.[/]");
            return 0;
        }
        Console.Error.WriteLine($"Verify FAILED for '{part}': {mismatches} mismatches out of {sample.Count} samples.");
        return 1;
    }

    // -------------------------------------------------------------------------
    static async Task<int> RunDrop(string[] args, string cfgPath)
    {
        string? part = Flag(args, "--partition");
        if (part is null)
        {
            Console.Error.WriteLine("Missing required --partition <name>.");
            return 1;
        }
        string? dumpFile = Flag(args, "--dump-file");
        if (dumpFile is null)
        {
            Console.Error.WriteLine("Missing required --dump-file <path>.");
            return 1;
        }
        if (!HasFlag(args, "--verified"))
        {
            Console.Error.WriteLine(
                $"Refusing to drop '{part}': --verified flag missing. " +
                $"Operator must confirm pg_restore --list \"{dumpFile}\" readability before dropping " +
                "(tool does not shell out to pg_restore).");
            return 1;
        }

        var cfg     = AppConfig.Load(cfgPath);
        var tracker = new ProgressTracker(cfg.Migrator.CheckpointFile);
        if (!tracker.Load() || !tracker.Progress.Partitions.TryGetValue(part, out var entry))
        {
            Console.Error.WriteLine(
                $"Refusing to drop '{part}': no checkpoint entry. Migrate and verify first. " +
                $"Operator must confirm pg_restore --list \"{dumpFile}\" readability before dropping " +
                "(tool does not shell out to pg_restore).");
            return 1;
        }
        if (!entry.Verified)
        {
            Console.Error.WriteLine(
                $"Refusing to drop '{part}': checkpoint Verified==false. Run verify first. " +
                $"Operator must confirm pg_restore --list \"{dumpFile}\" readability before dropping " +
                "(tool does not shell out to pg_restore).");
            return 1;
        }
        if (!File.Exists(dumpFile))
        {
            Console.Error.WriteLine(
                $"Refusing to drop '{part}': dump file '{dumpFile}' not found. " +
                $"Operator must confirm pg_restore --list \"{dumpFile}\" readability before dropping " +
                "(tool does not shell out to pg_restore).");
            return 1;
        }

        await using var conn = new NpgsqlConnection(cfg.Pg.ConnectionString);
        await conn.OpenAsync();
        var reader = new PgReader(conn, false);

        // Validate --partition against server list before use in any SQL.
        var names = await reader.ListPartitionsAsync();
        if (!names.Any(p => p.Name == part))
        {
            Console.Error.WriteLine($"Unknown partition '{part}'. Use list-partitions to see valid names.");
            return 1;
        }

        await using (var cmd = conn.CreateCommand())
        {
            cmd.CommandText = $"DROP TABLE \"{part}\";";
            await cmd.ExecuteNonQueryAsync();
        }
        tracker.Update(p => p.Partitions[part] = p.Partitions[part] with { Dropped = true, State = "dropped" });
        AnsiConsole.MarkupLine(
            $"[green]Dropped '{part}'. Dump '{dumpFile}' was present; operator confirmed " +
            $"pg_restore --list readability (tool does not shell out to pg_restore).[/]");
        return 0;
    }

    // -------------------------------------------------------------------------
    static int RunStatus(string cfgPath)
    {
        var cfg     = AppConfig.Load(cfgPath);
        var tracker = new ProgressTracker(cfg.Migrator.CheckpointFile);
        if (!tracker.Load())
        {
            AnsiConsole.MarkupLine("[grey]No checkpoint found.[/]");
            return 0;
        }

        var p = tracker.Progress;
        var table = new Table();
        table.AddColumn("Field");
        table.AddColumn("Value");
        table.AddRow("Phase",              p.Phase);
        table.AddRow("Started At",         p.StartedAt);
        table.AddRow("Migrated Rows",      $"{p.MigratedRows:N0}");
        table.AddRow("Skipped Rows",       $"{p.SkippedRows:N0}");
        table.AddRow("Completed Entities", $"{p.CompletedEntities.Count}");
        table.AddRow("Last Entity",        p.LastEntityId ?? "-");
        table.AddRow("Partitioning",       p.Partitioning);
        table.AddRow("Cast Strings",       p.CastStrings.ToString());
        if (p.WatermarkTs != 0)
            table.AddRow("Watermark",
                DateTimeOffset.FromUnixTimeMilliseconds(p.WatermarkTs).ToString("O"));
        AnsiConsole.Write(table);
        return 0;
    }

    // -------------------------------------------------------------------------
    static async Task PrintLoop(ProgressTracker tracker, CancellationToken ct)
    {
        try
        {
            while (!ct.IsCancellationRequested)
            {
                await Task.Delay(5_000, ct);
                var p = tracker.Progress;
                Console.Error.WriteLine(
                    $"[STATUS] phase={p.Phase} migrated={p.MigratedRows:N0} " +
                    $"skipped={p.SkippedRows:N0} entities_done={p.CompletedEntities.Count}");
            }
        }
        catch (OperationCanceledException) { }
    }

    // -------------------------------------------------------------------------
    static bool HasFlag(string[] args, string flag) =>
        args.Any(a => a.Equals(flag, StringComparison.OrdinalIgnoreCase));

    static string? Flag(string[] args, string name)
    {
        for (int i = 0; i < args.Length - 1; i++)
            if (args[i].Equals(name, StringComparison.OrdinalIgnoreCase))
                return args[i + 1];
        return null;
    }
}
