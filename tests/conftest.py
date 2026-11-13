from __future__ import annotations

from pathlib import Path

import pytest
import yaml

from lakehouse.config import DEFAULT_CONFIG, load_settings
from lakehouse.generator import batch_dates, generate
from lakehouse.pipeline import run_batch
from lakehouse.storage import TableStore


def small_config(tmp: Path, **generator) -> Path:
    raw = yaml.safe_load(DEFAULT_CONFIG.read_text())
    raw["generator"].update({"customers": 600, "merchants": 30, "transactions_per_day": 1000, **generator})
    path = tmp / "pipeline.yaml"
    path.write_text(yaml.safe_dump(raw, sort_keys=False))
    return path


@pytest.fixture
def cfg(tmp_path):
    return load_settings(small_config(tmp_path), root=tmp_path)


@pytest.fixture(scope="session")
def demo(tmp_path_factory):
    """One full pipeline run on a small synthetic dataset, shared by the end-to-end tests."""
    tmp = tmp_path_factory.mktemp("demo")
    cfg = load_settings(small_config(tmp), root=tmp)
    truth = generate(cfg)
    store = TableStore(cfg.path("lake"))
    runs = [run_batch(cfg, store, b) for b in batch_dates(cfg)]
    return cfg, truth, store, runs
