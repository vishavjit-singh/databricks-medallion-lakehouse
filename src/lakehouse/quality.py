"""Data-quality gates between bronze and silver.

Three levels, cheapest first:

1. **Schema gate (file level).** A file that is missing a contracted column is rejected whole:
   every row is quarantined with ``schema_drift:<columns>``. Unknown extra columns are not an error;
   they are already preserved in ``_rescued_data``.
2. **Row rules.** Malformed records, failed type casts and the rules in ``pipeline.yaml``
   (``not_null``, ``regex``, ``accepted_values``, ``range``, ``foreign_key``). A row that fails any
   rule goes to quarantine with *all* the reasons it failed, never silently dropped.
3. **Batch gate.** If the share of quarantined rows in a batch is above ``max_failure_rate`` the
   batch is **blocked**: nothing from it is published to silver, so a broken feed cannot leak half
   a day of data into reports. The rows stay in bronze and quarantine for investigation.

Null handling follows the usual convention: only ``not_null`` fails a null; the other rules skip it.
"""
from __future__ import annotations

import json
import re
from dataclasses import dataclass, field

import pandas as pd

from .config import Entity, Rule


def cast_column(s: pd.Series, kind: str) -> pd.Series:
    if kind == "string":
        vals = [v.strip() if isinstance(v, str) else None for v in s.tolist()]
        return pd.Series([v if v else None for v in vals], index=s.index, dtype="object")
    if kind == "date":
        parsed = pd.to_datetime(s, format="%Y-%m-%d", errors="coerce")
        return parsed.dt.date.astype("object").where(parsed.notna(), None)
    if kind == "timestamp":
        return pd.to_datetime(s, utc=True, format="ISO8601", errors="coerce")
    if kind == "decimal":
        return pd.to_numeric(s, errors="coerce").round(2)
    raise ValueError(kind)


def rule_failures(df: pd.DataFrame, rule: Rule, ref_keys: dict[str, set] | None = None) -> pd.Series:
    """Boolean Series: True where the row FAILS the rule."""
    col = df[rule.column]
    present = col.notna()
    if rule.type == "not_null":
        return ~present
    if rule.type == "regex":
        rx = re.compile(rule.pattern)
        return present & ~col.map(lambda v: bool(rx.search(str(v))) if v is not None else True)
    if rule.type == "accepted_values":
        return present & ~col.isin(list(rule.values))
    if rule.type == "range":
        bad = pd.Series(False, index=df.index)
        if rule.min is not None:
            bad |= col < rule.min
        if rule.max is not None:
            bad |= col > rule.max
        return present & bad.fillna(False)
    if rule.type == "foreign_key":
        keys = (ref_keys or {}).get(rule.references, set())
        return present & ~col.isin(keys)
    raise ValueError(rule.type)


@dataclass
class QualityResult:
    valid: pd.DataFrame                    # typed rows that passed every rule
    quarantine: pd.DataFrame               # failed rows: reasons + the raw record as JSON
    results: list[dict] = field(default_factory=list)   # one row per rule (and the gate) for the scorecard
    checked: int = 0
    duplicates_dropped: int = 0
    failure_rate: float = 0.0
    blocked: bool = False


def evaluate(entity: Entity, bronze_batch: pd.DataFrame, file_facts: dict[str, dict],
             ref_keys: dict[str, set], batch_id: str) -> QualityResult:
    cols = entity.columns
    raw = bronze_batch.copy()
    before = len(raw)
    raw = raw.drop_duplicates(subset=cols + ["_corrupt_record"], keep="first")   # exact re-deliveries
    dups = before - len(raw)
    raw = raw.reset_index(drop=True)

    typed = pd.DataFrame(index=raw.index)
    reasons: list[list[str]] = [[] for _ in range(len(raw))]

    def flag(mask: pd.Series, reason: str) -> int:
        idx = mask[mask].index
        for i in idx:
            reasons[i].append(reason)
        return len(idx)

    results: list[dict] = []

    def record(rule: str, rtype: str, column: str, failed: int, checked: int) -> None:
        results.append({"batch_id": batch_id, "entity": entity.name, "rule": rule, "rule_type": rtype,
                        "column": column, "checked": int(checked), "failed": int(failed)})

    # 1. schema gate (file level)
    missing = raw["_source_file"].map(lambda f: (file_facts.get(f, {}) or {}).get("missing_columns") or "")
    missing = missing.fillna("").astype(str).replace({"nan": ""})
    drift = missing != ""
    for f_missing in sorted(set(missing[drift])):
        flag(missing == f_missing, f"schema_drift:{f_missing}")
    record("schema_contract", "schema", "*", int(drift.sum()), len(raw))

    # 2a. malformed records
    corrupt = raw["_corrupt_record"].notna()
    record("well_formed", "parse", "*", flag(corrupt, "malformed_record"), len(raw))

    # 2b. type casts
    cast_failed = 0
    for col, kind in entity.schema.items():
        typed[col] = cast_column(raw[col], kind)
        if kind != "string":
            bad = raw[col].notna() & pd.Series(typed[col]).isna() & ~corrupt
            cast_failed += flag(bad, f"type_cast:{col}")
    record("types_castable", "cast", "*", cast_failed, len(raw))

    # 2c. configured rules (skip rows already known to be unparseable)
    parseable = ~corrupt & ~drift
    for rule in entity.rules:
        fails = rule_failures(typed, rule, ref_keys) & parseable
        record(rule.name, rule.type, rule.column, flag(fails, rule.name), int(parseable.sum()))

    reason_s = pd.Series([";".join(r) for r in reasons], index=raw.index, dtype="object")
    bad_rows = reason_s != ""
    failure_rate = float(bad_rows.mean()) if len(raw) else 0.0
    blocked = failure_rate > entity.max_failure_rate
    record("batch_gate", "gate", f"<= {entity.max_failure_rate:.0%}", int(bad_rows.sum()), len(raw))
    for r in results:
        r["gate_status"] = "BLOCKED" if blocked else "PASSED"

    meta = ["_source_file", "_line_no", "_batch_id", "_rescued_data"]
    valid = pd.concat([typed[~bad_rows], raw.loc[~bad_rows, meta]], axis=1).reset_index(drop=True)

    q = raw[bad_rows]
    quarantine = pd.DataFrame({
        "batch_id": batch_id,
        "entity": entity.name,
        "source_file": q["_source_file"].values,
        "line_no": q["_line_no"].astype("int64").values,
        "reasons": reason_s[bad_rows].values,
        "record": [json.dumps({c: r[c] for c in cols + ["_rescued_data", "_corrupt_record"] if pd.notna(r[c])},
                              sort_keys=True) for r in q.to_dict("records")],
    })
    return QualityResult(valid=valid, quarantine=quarantine, results=results, checked=len(raw),
                         duplicates_dropped=dups, failure_rate=failure_rate, blocked=blocked)


def latest_per_key(df: pd.DataFrame, key: str, order_by: str) -> tuple[pd.DataFrame, int]:
    """Keep the newest version of each key inside one batch (ties: the later line wins)."""
    if df.empty:
        return df, 0
    ordered = df.sort_values([key, order_by, "_line_no"], kind="mergesort", na_position="first")
    latest = ordered.drop_duplicates(subset=[key], keep="last")
    return latest.sort_values(key).reset_index(drop=True), len(df) - len(latest)
