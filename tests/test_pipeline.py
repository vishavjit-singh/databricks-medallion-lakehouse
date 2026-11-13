"""End to end on a small synthetic dataset: the generator knows the truth, the lake must match it."""
import pandas as pd
import pytest

from lakehouse.generator import GATE_TRIP_DAY, SCHEMA_DRIFT_DAY, batch_dates
from lakehouse.masking import hash_value
from lakehouse.pipeline import PiiLeakError, build_gold, run_batch


def test_expected_batches_are_blocked(demo):
    cfg, _, _, runs = demo
    blocked = {(r["batch_id"], n) for r in runs for n, e in r["entities"].items() if e["blocked"]}
    days = batch_dates(cfg)
    assert blocked == {(days[SCHEMA_DRIFT_DAY - 1], "merchants"), (days[GATE_TRIP_DAY - 1], "transactions")}


def test_silver_customers_match_the_truth(demo):
    cfg, truth, store, _ = demo
    cur = store.read("silver", "customers")
    cur = cur[cur["is_current"].astype(bool)].set_index("customer_id")
    assert set(cur.index) == set(truth.customers)
    for cid, c in truth.customers.items():
        assert cur.loc[cid, "city"] == c["city"] and cur.loc[cid, "kyc_tier"] == c["kyc_tier"], cid


def test_scd2_history_is_consistent(demo):
    _, _, store, _ = demo
    c = store.read("silver", "customers")
    assert c.groupby("customer_id")["is_current"].sum().eq(1).all()
    closed = c[~c["is_current"].astype(bool)]
    assert len(closed) > 0 and closed["valid_to"].notna().all()
    assert (closed["valid_to"] > closed["valid_from"]).all()


def test_merchants_rejected_by_schema_gate_arrive_after_the_resend(demo):
    _, truth, store, _ = demo
    m = store.read("silver", "merchants").set_index("merchant_id")
    assert set(m.index) == set(truth.merchants)
    assert all(m.loc[k, "category"] == v["category"] for k, v in truth.merchants.items())


def test_transactions_are_unique_and_refunds_win(demo):
    _, _, store, _ = demo
    t = store.read("silver", "transactions")
    assert t["txn_id"].is_unique
    assert (t["status"] == "refunded").sum() > 0
    assert t["currency"].isin(["INR", "USD", "EUR"]).all() and (t["amount"] > 0).all()
    assert (t["amount_inr"] >= t["amount"]).all()


def test_nothing_from_a_blocked_batch_reaches_silver(demo):
    cfg, _, store, _ = demo
    day = batch_dates(cfg)[GATE_TRIP_DAY - 1]
    t = store.read("silver", "transactions")
    assert not (t["_batch_id"] == day).any()
    q = store.read("silver", "_quarantine")
    assert (q[q["batch_id"] == day]["reasons"].str.contains("currency_supported")).sum() > 50


def test_gold_has_no_raw_pii(demo):
    cfg, truth, store, _ = demo
    dim = store.read("gold", "dim_customer")
    emails = {c["email"] for c in truth.customers.values()}
    assert not set(dim["email"]) & emails
    some = next(iter(truth.customers.values()))
    row = dim[dim["customer_id"] == some["customer_id"]].iloc[0]
    assert row["email"] == hash_value(some["email"], cfg.pii.salt())
    assert row["full_name"] == "***" and row["phone"].endswith(some["phone"][-4:])
    assert "date_of_birth" not in dim


def test_gold_revenue_reconciles_with_silver(demo):
    _, _, store, _ = demo
    rev = store.read("gold", "daily_merchant_revenue")
    t = store.read("silver", "transactions")
    paid = t[t["status"].isin(["captured", "refunded"])]
    assert rev["payments"].sum() == len(paid)
    assert rev["gross_inr"].sum() == pytest.approx(paid["amount_inr"].sum(), abs=0.05)
    assert rev["net_inr"].sum() == pytest.approx(t.loc[t["status"] == "captured", "amount_inr"].sum(), abs=0.05)


def test_rerunning_a_batch_changes_nothing(demo):
    cfg, _, store, _ = demo
    before = {n: store.read("silver", n) for n in ("customers", "merchants", "transactions", "_quarantine")}
    run_batch(cfg, store, batch_dates(cfg)[1])
    for n, df in before.items():
        after = store.read("silver", n)
        assert len(after) == len(df), n
    pd.testing.assert_frame_equal(
        before["customers"].sort_values(["customer_id", "valid_from"]).reset_index(drop=True),
        store.read("silver", "customers").sort_values(["customer_id", "valid_from"]).reset_index(drop=True))


def test_leak_scan_stops_publishing(demo, monkeypatch):
    cfg, _, store, _ = demo
    version = store.version("gold", "dim_customer")
    from lakehouse import gold

    real = gold.apply_policy
    forgot_email = lambda df, methods, salt: real(df, {k: v for k, v in methods.items() if k != "email"}, salt)  # noqa: E731
    monkeypatch.setattr(gold, "apply_policy", forgot_email)
    with pytest.raises(PiiLeakError):
        build_gold(cfg, store)
    assert store.version("gold", "dim_customer") == version
