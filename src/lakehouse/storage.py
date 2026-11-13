"""A minimal versioned table store for the local engine.

It borrows the one idea from Delta Lake that matters for correctness here: **a table is whatever
the latest commit file says it is**. Data files are written first; the commit
(``_log/<version>.json``) is created last with an exclusive create, so a crash leaves orphan data
files but never a half-written table, and two writers cannot both claim the same version.
Every commit records the operation and its metrics, which gives a ``history`` and time travel.

The Spark engine writes real Delta tables instead. This store only exists so ``make demo`` and the
tests run on a laptop without a JVM. Data files are Parquet when ``pyarrow`` is installed and CSV
otherwise; column kinds are kept in the commit so CSV round-trips keep their types.
"""
from __future__ import annotations

import json
from datetime import date, datetime, timezone
from pathlib import Path

import pandas as pd

LAYERS = ("bronze", "silver", "gold")


def default_format() -> str:
    try:
        import pyarrow  # noqa: F401
    except ImportError:
        return "csv"
    return "parquet"


def _kind(s: pd.Series) -> str:
    if isinstance(s.dtype, pd.DatetimeTZDtype) or pd.api.types.is_datetime64_any_dtype(s):
        return "timestamp"
    if pd.api.types.is_bool_dtype(s):
        return "bool"
    if pd.api.types.is_integer_dtype(s):
        return "int"
    if pd.api.types.is_float_dtype(s):
        return "float"
    sample = s.dropna()
    if len(sample) and isinstance(sample.iloc[0], date) and not isinstance(sample.iloc[0], datetime):
        return "date"
    return "string"


def _restore(df: pd.DataFrame, kinds: dict[str, str]) -> pd.DataFrame:
    for col, kind in kinds.items():
        if col not in df:
            continue
        if kind == "timestamp":
            df[col] = pd.to_datetime(df[col], utc=True, format="ISO8601")
        elif kind == "date":
            df[col] = pd.to_datetime(df[col], format="ISO8601").dt.date.where(df[col].notna(), None)
        elif kind == "int":
            df[col] = pd.to_numeric(df[col]).astype("Int64")
        elif kind == "float":
            df[col] = pd.to_numeric(df[col]).astype("float64")
        elif kind == "bool":
            df[col] = df[col].map({True: True, False: False, "True": True, "False": False}).astype("boolean")
        else:
            df[col] = df[col].astype("object").where(df[col].notna(), None)
    return df


class TableStore:
    def __init__(self, root: str | Path, fmt: str | None = None):
        self.root = Path(root)
        self.fmt = fmt or default_format()

    def table_path(self, layer: str, name: str) -> Path:
        if layer not in LAYERS:
            raise ValueError(f"unknown layer {layer}")
        return self.root / layer / name

    def _log(self, layer: str, name: str) -> Path:
        return self.table_path(layer, name) / "_log"

    def history(self, layer: str, name: str) -> list[dict]:
        log = self._log(layer, name)
        if not log.exists():
            return []
        return [json.loads(p.read_text()) for p in sorted(log.glob("*.json"))]

    def version(self, layer: str, name: str) -> int:
        h = self.history(layer, name)
        return h[-1]["version"] if h else -1

    def exists(self, layer: str, name: str) -> bool:
        return self.version(layer, name) >= 0

    def write(self, layer: str, name: str, df: pd.DataFrame, mode: str = "append",
              operation: str = "WRITE", metrics: dict | None = None) -> int:
        if mode not in ("append", "overwrite"):
            raise ValueError(mode)
        history = self.history(layer, name)
        version = history[-1]["version"] + 1 if history else 0
        files = [] if mode == "overwrite" or not history else list(history[-1]["files"])
        kinds = dict(history[-1]["kinds"]) if history and mode == "append" else {}
        added = []
        if len(df) or not files:
            path = self.table_path(layer, name)
            path.mkdir(parents=True, exist_ok=True)
            fname = f"part-{version:05d}.{self.fmt}"
            out = df.reset_index(drop=True)
            if self.fmt == "parquet":
                out.to_parquet(path / fname, index=False)
            else:
                out.to_csv(path / fname, index=False)
            files.append(fname)
            added.append(fname)
            kinds.update({c: _kind(df[c]) for c in df.columns})
        commit = {
            "version": version,
            "timestamp": datetime.now(timezone.utc).isoformat(timespec="seconds"),
            "operation": operation,
            "mode": mode,
            "files": files,
            "added": added,
            "kinds": kinds,
            "columns": list(df.columns) if mode == "overwrite" or not history else history[-1]["columns"],
            "metrics": {"rows_written": int(len(df)), **(metrics or {})},
        }
        log = self._log(layer, name)
        log.mkdir(parents=True, exist_ok=True)
        with (log / f"{version:020d}.json").open("x") as f:   # exclusive create = optimistic concurrency
            json.dump(commit, f, indent=1, default=str)
        return version

    def read(self, layer: str, name: str, version: int | None = None) -> pd.DataFrame:
        history = self.history(layer, name)
        if not history:
            raise FileNotFoundError(f"{layer}.{name} does not exist")
        commit = history[-1] if version is None else next(c for c in history if c["version"] == version)
        path = self.table_path(layer, name)
        frames = []
        for f in commit["files"]:
            p = path / f
            frames.append(pd.read_parquet(p) if p.suffix == ".parquet" else pd.read_csv(p, dtype=str,
                                                                                        keep_default_na=False,
                                                                                        na_values=[""]))
        frames = [f for f in frames if len(f.columns)]
        if not frames:
            return pd.DataFrame(columns=commit["columns"])
        df = pd.concat(frames, ignore_index=True) if len(frames) > 1 else frames[0]
        if any(p.endswith(".csv") for p in commit["files"]):
            df = _restore(df, commit["kinds"])
        return df

    def read_or_empty(self, layer: str, name: str, columns: list[str]) -> pd.DataFrame:
        return self.read(layer, name) if self.exists(layer, name) else pd.DataFrame(columns=columns)
