"""Command line: ``python -m lakehouse <command>``."""
from __future__ import annotations

import json
import logging
import shutil

import click

from .config import load_settings
from .generator import batch_dates, generate
from .masking import unity_catalog_sql
from .pipeline import load_runs, run_batch
from .storage import TableStore

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")


@click.group()
@click.option("--config", "config_path", type=click.Path(exists=True, dir_okay=False), default=None,
              help="Path to pipeline.yaml (defaults to config/pipeline.yaml).")
@click.pass_context
def cli(ctx, config_path):
    """Medallion lakehouse: bronze → silver → gold with quality gates and PII masking."""
    ctx.obj = load_settings(config_path)


@cli.command("generate")
@click.pass_obj
def generate_cmd(cfg):
    """Write synthetic landing files (one folder per day) for every entity."""
    shutil.rmtree(cfg.path("landing"), ignore_errors=True)
    truth = generate(cfg)
    click.echo(f"wrote {len(truth.files)} files to {cfg.path('landing')} "
               f"({len(truth.customers)} customers, {len(truth.merchants)} merchants)")


@cli.command("run")
@click.option("--batch", "batches", multiple=True, help="Landing date(s) to process; default: all of them.")
@click.option("--format", "fmt", type=click.Choice(["parquet", "csv"]), default=None)
@click.pass_obj
def run_cmd(cfg, batches, fmt):
    """Run batches through the reference (pandas) engine."""
    store = TableStore(cfg.path("lake"), fmt)
    for b in batches or batch_dates(cfg):
        s = run_batch(cfg, store, b)
        blocked = [n for n, e in s["entities"].items() if e.get("blocked")]
        click.echo(f"{b}: gold={s['gold']}" + (f"  BLOCKED by quality gate: {', '.join(blocked)}" if blocked else ""))


@cli.command("report")
@click.option("--out", default=None, help="Output folder (defaults to docs/images).")
@click.pass_obj
def report_cmd(cfg, out):
    """Render charts from the lake tables and run summaries."""
    from .report import render_all

    for p in render_all(cfg, TableStore(cfg.path("lake")), out):
        click.echo(f"wrote {p}")


@cli.command("summary")
@click.pass_obj
def summary_cmd(cfg):
    """Print the numbers quoted in the README, computed from the tables."""
    from .report import summary

    click.echo(json.dumps(summary(cfg, TableStore(cfg.path("lake"))), indent=2))


@cli.command("history")
@click.argument("table")
@click.pass_obj
def history_cmd(cfg, table):
    """Commit history of a table, e.g. ``silver.customers``."""
    layer, name = table.split(".", 1)
    for c in TableStore(cfg.path("lake")).history(layer, name):
        click.echo(f"v{c['version']:<3} {c['timestamp']}  {c['operation']:<17} {json.dumps(c['metrics'])}")


@cli.command("uc-sql")
@click.option("--catalog", default="main")
@click.option("--out", type=click.Path(dir_okay=False), default="sql/unity_catalog_masks.sql")
@click.pass_obj
def uc_sql_cmd(cfg, catalog, out):
    """Generate Unity Catalog column-mask SQL from the PII policy."""
    path = cfg.root / out
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(unity_catalog_sql(cfg.pii, catalog=catalog))
    click.echo(f"wrote {path}")


@cli.command("spark")
@click.option("--batch", "batches", multiple=True, help="Landing date(s); default: all of them.")
@click.option("--lake", default=None, help="Root path for Delta tables (default: paths.spark_lake).")
@click.option("--catalog", default=None, help="Unity Catalog name; tables become <catalog>.<layer>.<table>.")
@click.pass_obj
def spark_cmd(cfg, batches, lake, catalog):
    """Run batches with PySpark + Delta Lake (needs requirements-spark.txt and Java 17)."""
    from .spark.jobs import Tables, get_spark, run_batch as spark_run_batch

    spark = get_spark()
    t = Tables(spark, root=None if catalog else str(lake or cfg.path("spark_lake")), catalog=catalog)
    for b in batches or batch_dates(cfg):
        s = spark_run_batch(cfg, t, b)
        click.echo(f"{b}: gold={s['gold']}")
    for layer, name in (("silver", "customers"), ("silver", "transactions"), ("gold", "daily_merchant_revenue")):
        click.echo(f"{layer}.{name}: {t.read(layer, name).count()} rows, "
                   f"{t.delta(layer, name).history().count()} Delta versions")


@cli.command("runs")
@click.pass_obj
def runs_cmd(cfg):
    """Per-batch run summaries written by the reference engine."""
    for r in load_runs(cfg.path("lake")):
        click.echo(json.dumps(r))


def main():
    cli()
