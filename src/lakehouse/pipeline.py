"""Runs one batch (one landing date) through bronze → silver → gold with the reference engine."""
from __future__ import annotations

import json
import logging
from pathlib import Path

import pandas as pd

from . import bronze, gold, quality, silver
from .config import Settings
from .masking import raw_pii_values, scan_for_leaks
from .storage import TableStore

log = logging.getLogger("lakehouse")

QUARANTINE = "_quarantine"
DQ_RESULTS = "_dq_results"
QUARANTINE_COLUMNS = ["batch_id", "entity", "source_file", "line_no", "reasons", "record"]
DQ_COLUMNS = ["batch_id", "entity", "rule", "rule_type", "column", "checked", "failed", "gate_status"]


class PiiLeakError(RuntimeError):
    pass


def _replace_batch(store: TableStore, table: str, columns: list[str], new: pd.DataFrame, batch_id: str,
                   entity: str) -> None:
    """Idempotent write for audit tables: drop this batch+entity, then add the fresh rows."""
    old = store.read_or_empty("silver", table, columns)
    if len(old):
        old = old[~((old["batch_id"] == batch_id) & (old["entity"] == entity))]
    frames = [f for f in (old, new) if len(f)]
    df = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame(columns=columns)
    store.write("silver", table, df[columns], mode="overwrite", operation="REPLACE BATCH",
                metrics={"batch_id": batch_id, "entity": entity})


def ref_keys_for(cfg: Settings, store: TableStore, entity_name: str) -> dict[str, set]:
    keys: dict[str, set] = {}
    for rule in cfg.entities[entity_name].rules:
        if rule.type == "foreign_key":
            ref = cfg.entities[rule.ref_entity]
            df = store.read_or_empty("silver", ref.name, [rule.ref_column])
            keys[rule.references] = set(df[rule.ref_column].dropna())
    return keys


def run_batch(cfg: Settings, store: TableStore, batch_id: str) -> dict:
    summary: dict = {"batch_id": batch_id, "entities": {}}
    ingested = bronze.ingest(cfg, store, batch_id)
    facts = bronze.file_facts(store)

    for name in cfg.order:
        entity = cfg.entities[name]
        s = {"bronze_rows": ingested[name]["rows"], "files": ingested[name]["files"],
             "files_skipped": ingested[name]["files_skipped"]}
        if not store.exists("bronze", name):
            summary["entities"][name] = s
            continue
        b = store.read("bronze", name)
        batch = b[b["_batch_id"] == batch_id].reset_index(drop=True)
        s["bronze_batch_rows"] = len(batch)
        q = quality.evaluate(entity, batch, facts, ref_keys_for(cfg, store, name), batch_id)
        _replace_batch(store, QUARANTINE, QUARANTINE_COLUMNS, q.quarantine, batch_id, name)
        _replace_batch(store, DQ_RESULTS, DQ_COLUMNS, pd.DataFrame(q.results, columns=DQ_COLUMNS), batch_id, name)
        s.update(checked=q.checked, duplicates_dropped=q.duplicates_dropped, quarantined=len(q.quarantine),
                 failure_rate=round(q.failure_rate, 4), blocked=q.blocked)
        if q.blocked:
            log.warning("%s %s: quality gate BLOCKED the batch (%.1f%% of rows failed, limit %.0f%%)",
                        batch_id, name, 100 * q.failure_rate, 100 * entity.max_failure_rate)
            s.update(superseded_in_batch=0, inserted=0, updated=0, unchanged=0, stale=0)
        else:
            latest, superseded = quality.latest_per_key(q.valid, entity.primary_key, entity.order_by)
            latest = silver.conform(cfg, entity, latest)
            cols = silver.silver_columns(cfg, entity)
            existing = store.read_or_empty("silver", name, cols)
            incoming = latest[[c for c in cols if c in latest.columns]]
            merged, m = silver.merge(existing, incoming, entity)
            store.write("silver", name, merged[cols], mode="overwrite", operation="MERGE",
                        metrics={"batch_id": batch_id, **m})
            s.update(superseded_in_batch=superseded, **m)
        s["silver_rows"] = len(store.read("silver", name)) if store.exists("silver", name) else 0
        summary["entities"][name] = s
        log.info("%s %-12s bronze=%-5d quarantined=%-4d dups=%-3d inserted=%-5d updated=%-4d %s", batch_id, name,
                 s["bronze_batch_rows"], s["quarantined"], s["duplicates_dropped"], s["inserted"], s["updated"],
                 "BLOCKED" if q.blocked else "ok")

    summary["gold"] = build_gold(cfg, store)
    _write_run(cfg, store, summary)
    return summary


def build_gold(cfg: Settings, store: TableStore) -> dict:
    tx = store.read("silver", "transactions")
    merchants = store.read("silver", "merchants")
    customers = store.read("silver", "customers")
    tables = {
        "daily_merchant_revenue": gold.daily_merchant_revenue(tx, merchants),
        "dim_customer": gold.dim_customer(cfg, customers, tx),
        "dq_scorecard": gold.dq_scorecard(store.read("silver", DQ_RESULTS)),
    }
    findings = scan_for_leaks(tables, raw_pii_values(customers, cfg.pii))
    if findings:
        raise PiiLeakError(f"raw PII found in gold, nothing was published: {findings}")
    for name, df in tables.items():
        store.write("gold", name, df, mode="overwrite", operation="CREATE OR REPLACE")
    return {name: len(df) for name, df in tables.items()} | {"pii_leak_findings": 0}


def _write_run(cfg: Settings, store: TableStore, summary: dict) -> None:
    runs = store.root / "_runs"
    runs.mkdir(parents=True, exist_ok=True)
    (runs / f"{summary['batch_id']}.json").write_text(json.dumps(summary, indent=2, default=str))


def load_runs(lake: Path) -> list[dict]:
    return [json.loads(p.read_text()) for p in sorted((Path(lake) / "_runs").glob("*.json"))]
