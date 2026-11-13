"""The Spark + Delta engine must produce the same lake as the reference engine.

Runs in the ``spark`` workflow (needs pyspark, delta-spark and Java 17): ``pytest -m spark tests/spark``.
"""
from __future__ import annotations

import pandas as pd
import pytest

pytest.importorskip("pyspark")
pytest.importorskip("delta")

from lakehouse.config import load_settings  # noqa: E402
from lakehouse.generator import batch_dates, generate  # noqa: E402
from lakehouse.pipeline import run_batch  # noqa: E402
from lakehouse.storage import TableStore  # noqa: E402
from tests.conftest import small_config  # noqa: E402

pytestmark = pytest.mark.spark


@pytest.fixture(scope="module")
def lakes(tmp_path_factory):
    from lakehouse.spark.jobs import Tables, get_spark
    from lakehouse.spark.jobs import run_batch as spark_run_batch

    tmp = tmp_path_factory.mktemp("parity")
    cfg = load_settings(small_config(tmp, transactions_per_day=400, customers=300, merchants=20), root=tmp)
    generate(cfg)
    store = TableStore(cfg.path("lake"))
    spark = get_spark()
    t = Tables(spark, root=str(cfg.path("spark_lake")))
    py_runs, sp_runs = [], []
    for b in batch_dates(cfg):
        py_runs.append(run_batch(cfg, store, b))
        sp_runs.append(spark_run_batch(cfg, t, b))
    return cfg, store, t, py_runs, sp_runs


def _spark(t, layer, name) -> pd.DataFrame:
    return t.read(layer, name).toPandas()


def test_gate_decisions_and_counts_match(lakes):
    _, _, _, py_runs, sp_runs = lakes
    for p, s in zip(py_runs, sp_runs):
        for name, e in s["silver"].items():
            ref = p["entities"][name]
            for k in ("checked", "duplicates_dropped", "quarantined", "blocked", "inserted", "updated", "stale"):
                assert e[k] == ref[k], (p["batch_id"], name, k)


def test_dq_results_match(lakes):
    _, store, t, _, _ = lakes
    cols = ["batch_id", "entity", "rule", "checked", "failed", "gate_status"]
    py = store.read("silver", "_dq_results")[cols].astype(str).sort_values(cols).reset_index(drop=True)
    sp = _spark(t, "silver", "_dq_results")[cols].astype(str).sort_values(cols).reset_index(drop=True)
    pd.testing.assert_frame_equal(py, sp)


def test_quarantine_matches(lakes):
    _, store, t, _, _ = lakes
    cols = ["batch_id", "entity", "source_file", "line_no", "reasons"]
    py = store.read("silver", "_quarantine")[cols].astype(str).sort_values(cols).reset_index(drop=True)
    sp = _spark(t, "silver", "_quarantine")[cols].astype(str).sort_values(cols).reset_index(drop=True)
    pd.testing.assert_frame_equal(py, sp)


def test_silver_customers_scd2_match(lakes):
    _, store, t, _, _ = lakes
    cols = ["customer_id", "email", "city", "kyc_tier", "valid_from", "valid_to", "is_current"]
    py = store.read("silver", "customers")[cols]
    sp = _spark(t, "silver", "customers")[cols]
    for df in (py, sp):
        for c in ("valid_from", "valid_to"):
            df[c] = pd.to_datetime(df[c], utc=True).dt.strftime("%Y-%m-%dT%H:%M:%S").fillna("")
        df["is_current"] = df["is_current"].astype(bool)
    key = ["customer_id", "valid_from"]
    pd.testing.assert_frame_equal(py.sort_values(key).reset_index(drop=True).astype(str),
                                  sp.sort_values(key).reset_index(drop=True).astype(str))


def test_silver_transactions_match(lakes):
    _, store, t, _, _ = lakes
    py = store.read("silver", "transactions").set_index("txn_id").sort_index()
    sp = _spark(t, "silver", "transactions").set_index("txn_id").sort_index()
    assert list(py.index) == list(sp.index)
    assert (py["status"] == sp["status"]).all()
    assert (py["amount_inr"].astype(float) - sp["amount_inr"].astype(float)).abs().max() < 0.011


def test_gold_matches(lakes):
    _, store, t, _, _ = lakes
    key = ["txn_date", "category"]
    py = store.read("gold", "daily_merchant_revenue").sort_values(key).reset_index(drop=True)
    sp = _spark(t, "gold", "daily_merchant_revenue").sort_values(key).reset_index(drop=True)
    assert py[key + ["payments", "failed", "refunds", "paying_customers"]].astype(str).equals(
        sp[key + ["payments", "failed", "refunds", "paying_customers"]].astype(str))
    for c in ("gross_inr", "refunded_inr", "net_inr"):
        assert (py[c].astype(float) - sp[c].astype(float)).abs().max() < 0.05, c

    cols = ["customer_id", "full_name", "email", "phone", "kyc_tier", "payments", "versions", "date_of_birth_year"]
    py = store.read("gold", "dim_customer")[cols].sort_values("customer_id").reset_index(drop=True)
    sp = _spark(t, "gold", "dim_customer")[cols].sort_values("customer_id").reset_index(drop=True)
    pd.testing.assert_frame_equal(py.astype(str), sp.astype(str))
