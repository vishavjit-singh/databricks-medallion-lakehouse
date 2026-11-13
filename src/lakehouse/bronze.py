"""Bronze: land every source file as-is, with lineage, and never lose a byte.

* All business columns are kept as strings (schema-on-read); typing happens in silver.
* Columns the contract does not know go to ``_rescued_data`` as JSON instead of being dropped.
* Lines that do not parse go to ``_corrupt_record`` with the raw text.
* An ingest manifest (file path + SHA-256) makes ingestion idempotent: re-running a batch or
  re-delivering an identical file adds nothing. This is what Auto Loader's checkpoint does on Databricks.
"""
from __future__ import annotations

import csv
import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

from .config import Entity, Settings
from .storage import TableStore

META = ["_rescued_data", "_corrupt_record", "_source_file", "_line_no", "_batch_id", "_ingested_at"]
MANIFEST = "_ingest_manifest"
MANIFEST_COLUMNS = ["source_file", "sha256", "entity", "batch_id", "rows", "missing_columns", "extra_columns",
                    "corrupt_rows", "ingested_at"]


def bronze_columns(entity: Entity) -> list[str]:
    return entity.columns + META


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def parse_file(path: Path, entity: Entity) -> tuple[list[dict], dict]:
    """Parse one landing file into bronze rows (all values str | None) plus file-level facts."""
    expected = entity.columns
    rows: list[dict] = []
    seen_cols: set[str] = set()
    corrupt = 0
    if entity.format == "csv":
        with path.open(newline="") as f:
            reader = csv.DictReader(f)
            seen_cols = set(reader.fieldnames or [])
            for i, rec in enumerate(reader, start=2):
                rows.append(_shape(rec, expected, i))
    elif entity.format == "jsonl":
        for i, line in enumerate(path.read_text().splitlines(), start=1):
            if not line.strip():
                continue
            try:
                rec = json.loads(line)
                if not isinstance(rec, dict):
                    raise ValueError("not an object")
            except ValueError:
                corrupt += 1
                rows.append({**{c: None for c in expected}, "_rescued_data": None, "_corrupt_record": line,
                             "_line_no": i})
                continue
            seen_cols |= set(rec)
            rows.append(_shape(rec, expected, i))
    else:
        raise ValueError(f"unsupported format {entity.format}")
    info = {
        "missing_columns": ",".join(sorted(set(expected) - seen_cols)) if rows else "",
        "extra_columns": ",".join(sorted(seen_cols - set(expected))),
        "corrupt_rows": corrupt,
    }
    return rows, info


def _shape(rec: dict, expected: list[str], line_no: int) -> dict:
    out = {}
    for c in expected:
        v = rec.get(c)
        out[c] = None if v is None or v == "" else str(v)
    extra = {k: v for k, v in rec.items() if k not in expected}
    out["_rescued_data"] = json.dumps(extra, sort_keys=True) if extra else None
    out["_corrupt_record"] = None
    out["_line_no"] = line_no
    return out


def landing_files(cfg: Settings, entity: str, batch_id: str) -> list[Path]:
    folder = cfg.path("landing") / entity / batch_id
    return sorted(p for p in folder.glob("*") if p.is_file()) if folder.exists() else []


def ingest(cfg: Settings, store: TableStore, batch_id: str) -> dict[str, dict]:
    """Append this batch's landing files to bronze. Returns per-entity ingest metrics."""
    manifest = store.read_or_empty("bronze", MANIFEST, MANIFEST_COLUMNS)
    known = set(zip(manifest["source_file"], manifest["sha256"])) if len(manifest) else set()
    now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    results: dict[str, dict] = {}
    new_manifest = []
    for name in cfg.order:
        entity = cfg.entities[name]
        frames, files, skipped = [], [], 0
        for path in landing_files(cfg, name, batch_id):
            rel = str(path.relative_to(cfg.path("landing")))
            digest = _sha256(path)
            if (rel, digest) in known:
                skipped += 1
                continue
            rows, info = parse_file(path, entity)
            df = pd.DataFrame(rows, columns=entity.columns + ["_rescued_data", "_corrupt_record", "_line_no"])
            df["_source_file"] = rel
            df["_batch_id"] = batch_id
            df["_ingested_at"] = now
            frames.append(df[bronze_columns(entity)])
            files.append(rel)
            new_manifest.append({"source_file": rel, "sha256": digest, "entity": name, "batch_id": batch_id,
                                 "rows": len(df), **info, "ingested_at": now})
        rows_in = 0
        if frames:
            df = pd.concat(frames, ignore_index=True)
            df["_line_no"] = df["_line_no"].astype("int64")
            rows_in = len(df)
            store.write("bronze", name, df, mode="append", operation="INGEST",
                        metrics={"batch_id": batch_id, "files": files})
        results[name] = {"files": len(files), "files_skipped": skipped, "rows": rows_in}
    if new_manifest:
        store.write("bronze", MANIFEST, pd.DataFrame(new_manifest, columns=MANIFEST_COLUMNS), mode="append",
                    operation="INGEST")
    return results


def file_facts(store: TableStore) -> dict[str, dict]:
    """source_file -> manifest row (missing/extra columns etc.)."""
    m = store.read_or_empty("bronze", MANIFEST, MANIFEST_COLUMNS)
    return {r["source_file"]: r for r in m.to_dict("records")}
