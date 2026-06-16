import json
import logging
import os
import sys
from datetime import datetime

import click
import psycopg2
import psycopg2.extras
from rich.console import Console
from rich.panel import Panel
from rich.table import Table

from migrator.config import load_config
from migrator.orchestrator import Orchestrator
from migrator.pg_reader import PgReader
from migrator.progress import ProgressTracker
from migrator.scylla_writer import ScyllaWriter

console = Console()
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("migration_errors.log", encoding="utf-8"),
    ],
)
logger = logging.getLogger("main")


def _get_config(config_path: str):
    return load_config(config_path)


def _connect_pg(cfg):
    return psycopg2.connect(
        host=cfg.pg.host,
        port=cfg.pg.port,
        dbname=cfg.pg.db,
        user=cfg.pg.user,
        password=cfg.pg.password,
    )


def _connect_scylla(cfg):
    return ScyllaWriter.connect(cfg.scylla.host, cfg.scylla.port, cfg.scylla.keyspace)


@click.group()
@click.option("--config", default="config.yaml", show_default=True, help="Config file path")
@click.pass_context
def cli(ctx, config):
    ctx.ensure_object(dict)
    ctx.obj["config_path"] = config


@cli.command("init-schema")
@click.pass_context
def init_schema(ctx):
    """Create ScyllaDB keyspace and 3 timeseries tables."""
    cfg = _get_config(ctx.obj["config_path"])
    console.print(Panel("[bold cyan]Initializing ScyllaDB schema...[/bold cyan]"))
    try:
        writer = _connect_scylla(cfg)
        writer.init_schema()
        console.print("[green]✅ Schema created successfully![/green]")
        console.print(f"   Keyspace : [bold]{cfg.scylla.keyspace}[/bold]")
        console.print("   Tables   : ts_kv_cf, ts_kv_partitions_cf, ts_kv_latest_cf")
    except Exception as e:
        console.print(f"[red]❌ Failed: {e}[/red]")
        sys.exit(1)


@cli.command("start")
@click.option("--resume", is_flag=True, help="Resume from checkpoint")
@click.option("--historical-only", is_flag=True, help="Run Phase 0+1 only (skip live sync)")
@click.option("--cast-strings", is_flag=True, help="Cast str_v to long_v/dbl_v if possible")
@click.option("--partitioning", default=None, help="Partition strategy (MONTHS/DAYS/HOURS/...)")
@click.pass_context
def start(ctx, resume, historical_only, cast_strings, partitioning):
    """Run full migration: Phase 0 (preload) -> Phase 1 (historical) -> Phase 2 (live sync)."""
    cfg = _get_config(ctx.obj["config_path"])

    # CLI flags override config
    if cast_strings:
        cfg.cast_strings = True
    if partitioning:
        cfg.partitioning = partitioning

    console.print(
        Panel(
            f"[bold cyan]TB PostgreSQL → ScyllaDB Migrator[/bold cyan]\n\n"
            f"  PostgreSQL : {cfg.pg.host}:{cfg.pg.port}/{cfg.pg.db}\n"
            f"  ScyllaDB   : {cfg.scylla.host}:{cfg.scylla.port}/{cfg.scylla.keyspace}\n"
            f"  Partitioning: {cfg.partitioning}\n"
            f"  Cast strings: {cfg.cast_strings}\n"
            f"  Batch size  : {cfg.batch_size}",
            title="Migration Config",
        )
    )

    pg_conn = None
    try:
        console.print("[yellow]Connecting to PostgreSQL...[/yellow]")
        pg_conn = _connect_pg(cfg)
        console.print("[green]✅ PostgreSQL connected[/green]")

        console.print("[yellow]Connecting to ScyllaDB...[/yellow]")
        writer = _connect_scylla(cfg)
        console.print("[green]✅ ScyllaDB connected[/green]")

        reader = PgReader(pg_conn)
        tracker = ProgressTracker(cfg.checkpoint_file)

        orch = Orchestrator(cfg, reader, writer, tracker)
        orch.run(historical_only=historical_only, resume=resume)

        console.print("[bold green]Migration complete![/bold green]")
        _print_switchover_instructions(cfg)

    except KeyboardInterrupt:
        console.print("\n[yellow]Interrupted. Progress saved. Use --resume to continue.[/yellow]")
    except Exception as e:
        console.print(f"[red]❌ Migration failed: {e}[/red]")
        logger.exception("Migration failed")
        sys.exit(1)
    finally:
        if pg_conn:
            pg_conn.close()


@cli.command("status")
@click.pass_context
def status(ctx):
    """Show current migration status from checkpoint file."""
    cfg = _get_config(ctx.obj["config_path"])
    tracker = ProgressTracker(cfg.checkpoint_file)

    if not tracker.load():
        console.print("[yellow]No checkpoint found. Migration has not started.[/yellow]")
        return

    p = tracker.progress
    table = Table(title="Migration Status", show_header=False)
    table.add_column("Field", style="cyan")
    table.add_column("Value", style="white")

    table.add_row("Phase", p.phase)
    table.add_row("Started at", p.started_at)
    table.add_row("Partitioning", p.partitioning)
    table.add_row("Cast strings", str(p.cast_strings))
    table.add_row("Migrated rows", f"{p.migrated_rows:,}")
    table.add_row("Skipped rows", f"{p.skipped_rows:,}")
    table.add_row("Last entity ID", p.last_entity_id or "—")
    if p.watermark_ts:
        from datetime import datetime, timezone
        wm_dt = datetime.fromtimestamp(p.watermark_ts / 1000, tz=timezone.utc)
        table.add_row("Watermark", wm_dt.strftime("%Y-%m-%d %H:%M:%S UTC"))

    console.print(table)


def _print_switchover_instructions(cfg):
    instructions = f"""[bold yellow]SWITCHOVER INSTRUCTIONS[/bold yellow]

1. ThingsBoard ni to'xtatish:
   [cyan]docker compose stop thingsboard-ce[/cyan]

2. docker-compose.yml dagi thingsboard-ce ga qo'shing:
   [cyan]DATABASE_TS_TYPE: cassandra
   TS_KV_PARTITIONING: {cfg.partitioning}
   CASSANDRA_URL: {cfg.scylla.host}:{cfg.scylla.port}
   CASSANDRA_KEYSPACE_NAME: {cfg.scylla.keyspace}[/cyan]

3. TB ni qayta ishga tushirish:
   [cyan]docker compose -f docker-compose.yml -f docker-compose.scylla.yml up -d thingsboard-ce[/cyan]

4. Migrator ni to'xtatish:
   [cyan]docker compose -f docker-compose.yml -f docker-compose.scylla.yml stop tb-migrator[/cyan]
"""
    console.print(Panel(instructions, title="Keyingisi"))


if __name__ == "__main__":
    cli()
