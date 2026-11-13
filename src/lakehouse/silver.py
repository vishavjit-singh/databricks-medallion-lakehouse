"""Silver: typed, validated, de-duplicated, conformed tables, merged idempotently.

* ``scd2`` (customers): a change to a tracked column closes the current version
  (``valid_to``, ``is_current = false``) and opens a new one. Re-delivering the same data or an
  older version changes nothing. Changes to untracked columns are ignored.
* ``upsert`` (merchants, transactions): the newest ``updated_at`` per key wins, so a refund that
  arrives three days later replaces the original ``captured`` row, and a stale replay cannot
  overwrite newer data.

On Databricks both are a single ``MERGE INTO`` on a Delta table (see ``lakehouse/spark/jobs.py``).
"""
from __future__ import annotations

import pandas as pd

from .config import Entity, Settings

SCD2_COLUMNS = ["valid_from", "valid_to", "is_current"]
LINEAGE = ["_batch_id", "_source_file"]


def silver_columns(cfg: Settings, entity: Entity) -> list[str]:
    cols = list(entity.columns)
    if entity.name == "transactions":
        cols.append("amount_inr")
    cols += LINEAGE
    if entity.write_mode == "scd2":
        cols += SCD2_COLUMNS
    return cols


def conform(cfg: Settings, entity: Entity, df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    if entity.name == "transactions":
        rate = out["currency"].map(cfg.fx_to_inr)
        out["amount_inr"] = (out["amount"] * rate).round(2)
    return out


def _differs(a: pd.Series, b: pd.Series) -> pd.Series:
    return (a != b) & ~(a.isna() & b.isna())


def scd2_merge(existing: pd.DataFrame, incoming: pd.DataFrame, entity: Entity) -> tuple[pd.DataFrame, dict]:
    key, ts = entity.primary_key, entity.order_by
    tracked = list(entity.tracked_columns)
    incoming = incoming.copy()
    if existing.empty:
        incoming["valid_from"] = incoming[ts]
        incoming["valid_to"] = pd.NaT
        incoming["valid_to"] = pd.to_datetime(incoming["valid_to"], utc=True)
        incoming["is_current"] = True
        return incoming, {"inserted": len(incoming), "updated": 0, "unchanged": 0, "stale": 0}

    current = existing[existing["is_current"].astype(bool)]
    cur = current[[key, "valid_from"] + tracked].rename(columns={c: f"{c}__cur" for c in ["valid_from"] + tracked})
    m = incoming.merge(cur, on=key, how="left", indicator=True)
    matched = m["_merge"] == "both"
    stale = matched & (m[ts] <= m["valid_from__cur"])
    changed = pd.Series(False, index=m.index)
    for c in tracked:
        changed |= _differs(m[c], m[f"{c}__cur"])
    changed &= matched & ~stale
    new = ~matched

    closing = m.loc[changed, [key, ts]].rename(columns={ts: "__closed_at"})
    out = existing.merge(closing, on=key, how="left")
    close_mask = out["__closed_at"].notna() & out["is_current"].astype(bool)
    out.loc[close_mask, "valid_to"] = out.loc[close_mask, "__closed_at"]
    out.loc[close_mask, "is_current"] = False
    out = out.drop(columns="__closed_at")

    inserts = m.loc[new | changed, list(incoming.columns)].copy()
    inserts["valid_from"] = inserts[ts]
    inserts["valid_to"] = pd.to_datetime(pd.Series(pd.NaT, index=inserts.index), utc=True)
    inserts["is_current"] = True
    result = pd.concat([out, inserts[out.columns]], ignore_index=True)
    result["is_current"] = result["is_current"].astype(bool)
    result = result.sort_values([key, "valid_from"]).reset_index(drop=True)
    metrics = {"inserted": int(new.sum()), "updated": int(changed.sum()),
               "unchanged": int((matched & ~changed & ~stale).sum()), "stale": int(stale.sum())}
    return result, metrics


def upsert(existing: pd.DataFrame, incoming: pd.DataFrame, entity: Entity) -> tuple[pd.DataFrame, dict]:
    key, ts = entity.primary_key, entity.order_by
    if existing.empty:
        return incoming.sort_values(key).reset_index(drop=True), {"inserted": len(incoming), "updated": 0,
                                                                   "unchanged": 0, "stale": 0}
    cur = existing.set_index(key)
    inc = incoming.set_index(key)
    common = inc.index.intersection(cur.index)
    newer = inc.loc[common, ts] >= cur.loc[common, ts]
    metrics = {"inserted": int(len(inc.index.difference(cur.index))), "updated": int(newer.sum()),
               "unchanged": 0, "stale": int((~newer).sum())}
    take = inc.loc[inc.index.difference(cur.index).append(common[newer.values])]
    keep = cur.drop(index=common[newer.values])
    result = pd.concat([keep, take[keep.columns]]).reset_index().sort_values(key).reset_index(drop=True)
    return result, metrics


def merge(existing: pd.DataFrame, incoming: pd.DataFrame, entity: Entity) -> tuple[pd.DataFrame, dict]:
    if entity.write_mode == "scd2":
        return scd2_merge(existing, incoming, entity)
    if entity.write_mode == "upsert":
        return upsert(existing, incoming, entity)
    raise ValueError(entity.write_mode)
