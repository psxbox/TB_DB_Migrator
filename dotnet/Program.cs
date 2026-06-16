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
            default:
                Console.Error.WriteLine("Usage: tbmigrator <init-schema|start|status> [--config config.yaml]");
                Console.Error.WriteLine("  start [--resume] [--workers N] [--historical-only] [--config FILE]");
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
        var scylla  = ScyllaWriter.Connect(
            cfg.Scylla.Host, cfg.Scylla.Port,
            cfg.Scylla.Keyspace, cfg.Migrator.ScyllaConcurrency);

        scylla.InitSchema();

        var tracker = new ProgressTracker(cfg.Migrator.CheckpointFile);
        var orch    = new Orchestrator(cfg, reader, scylla, tracker);

        _ = Task.Run(() => PrintLoop(tracker, cts.Token));

        try
        {
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
