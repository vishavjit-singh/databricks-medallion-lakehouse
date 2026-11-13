import csv
import json

from lakehouse.generator import GATE_TRIP_DAY, SCHEMA_DRIFT_DAY, batch_dates, generate


def test_same_seed_same_files(cfg, tmp_path):
    generate(cfg)
    first = {p: p.read_bytes() for p in sorted(cfg.path("landing").rglob("*.*"))}
    generate(cfg)
    second = {p: p.read_bytes() for p in sorted(cfg.path("landing").rglob("*.*"))}
    assert first == second
    assert len(first) == 3 * cfg.generator.days


def test_schema_drift_day_renames_a_column(cfg):
    generate(cfg)
    day = batch_dates(cfg)[SCHEMA_DRIFT_DAY - 1]
    with (cfg.path("landing") / "merchants" / day / f"merchants_{day}.csv").open() as f:
        header = next(csv.reader(f))
    assert "merchant_category" in header and "category" not in header


def test_gate_trip_day_has_lower_case_currencies(cfg):
    generate(cfg)
    day = batch_dates(cfg)[GATE_TRIP_DAY - 1]
    lines = (cfg.path("landing") / "transactions" / day / f"transactions_{day}.jsonl").read_text().splitlines()
    parsed = []
    for line in lines:
        try:
            parsed.append(json.loads(line))
        except ValueError:
            pass
    lower = sum(1 for r in parsed if isinstance(r, dict) and str(r.get("currency", "")).islower())
    assert lower / len(parsed) > 0.05


def test_truth_contains_only_valid_customers(cfg):
    truth = generate(cfg)
    assert all(c["customer_id"] and "@" in c["email"] for c in truth.customers.values())
    g = cfg.generator
    first = int(g.customers * 0.85)
    assert len(truth.customers) == first + (g.customers - first) // (g.days - 1) * (g.days - 1)
