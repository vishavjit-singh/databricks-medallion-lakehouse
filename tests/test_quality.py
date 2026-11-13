import pandas as pd
import pytest

from lakehouse.config import Rule
from lakehouse.quality import cast_column, evaluate, latest_per_key, rule_failures


def bronze_rows(entity, rows, source="f.csv"):
    df = pd.DataFrame([{c: r.get(c) for c in entity.columns} for r in rows], columns=entity.columns)
    df["_rescued_data"] = None
    df["_corrupt_record"] = [r.get("_corrupt_record") for r in rows]
    df["_source_file"] = source
    df["_line_no"] = range(2, len(rows) + 2)
    df["_batch_id"] = "2026-11-01"
    return df


def merchant(mid="M1", **kw):
    return {"merchant_id": mid, "merchant_name": "Shop", "category": "grocery", "city": "Delhi",
            "updated_at": "2026-11-01T10:00:00Z", **kw}


def test_casts():
    assert cast_column(pd.Series([" a ", "", None]), "string").tolist() == ["a", None, None]
    d = cast_column(pd.Series(["2026-11-01", "nope", None]), "date")
    assert str(d.iloc[0]) == "2026-11-01" and d.iloc[1] is None
    ts = cast_column(pd.Series(["2026-11-01T10:00:00Z", "yesterday"]), "timestamp")
    assert ts.iloc[0].hour == 10 and pd.isna(ts.iloc[1])
    assert cast_column(pd.Series(["10.256", "x"]), "decimal").iloc[0] == 10.26


@pytest.mark.parametrize("rule, values, expected", [
    (Rule("r", "not_null", "c"), ["a", None], [False, True]),
    (Rule("r", "regex", "c", pattern=r"^[^@\s]+@[^@\s]+\.[a-z]{2,}$"), ["a@b.com", "a at b.com", None],
     [False, True, False]),
    (Rule("r", "accepted_values", "c", values=("INR", "USD")), ["INR", "inr", None], [False, True, False]),
    (Rule("r", "range", "c", min=0.01, max=100), [5.0, 0.0, 101.0, None], [False, True, True, False]),
    (Rule("r", "foreign_key", "c", references="customers.customer_id"), ["C1", "C9", None], [False, True, False]),
])
def test_rule_failures(rule, values, expected):
    df = pd.DataFrame({"c": values})
    assert rule_failures(df, rule, {"customers.customer_id": {"C1"}}).tolist() == expected


def test_valid_rows_pass_and_exact_duplicates_are_dropped(cfg):
    e = cfg.entities["merchants"]
    q = evaluate(e, bronze_rows(e, [merchant("M1"), merchant("M2"), merchant("M2")]), {}, {}, "2026-11-01")
    assert len(q.valid) == 2 and q.quarantine.empty
    assert q.duplicates_dropped == 1 and not q.blocked
    assert q.valid["updated_at"].dt.tz is not None


def test_failed_rows_carry_every_reason(cfg):
    e = cfg.entities["transactions"]
    bad = {"txn_id": None, "customer_id": "C9", "merchant_id": "M1", "amount": "-5", "currency": "GBP",
           "status": "captured", "channel": "app", "txn_ts": "2026-11-01T10:00:00Z",
           "updated_at": "2026-11-01T10:00:00Z"}
    good = dict(bad, txn_id="T1", customer_id="C1", amount="5", currency="INR")
    refs = {"customers.customer_id": {"C1"}, "merchants.merchant_id": {"M1"}}
    q = evaluate(e, bronze_rows(e, [good] * 1 + [bad]), {}, refs, "b")
    assert q.quarantine["reasons"].tolist() == ["txn_id_not_null;amount_positive;currency_supported;customer_exists"]
    assert len(q.valid) == 1
    results = {r["rule"]: r["failed"] for r in q.results}
    assert results["currency_supported"] == 1 and results["status_allowed"] == 0


def test_type_cast_failures_are_quarantined(cfg):
    e = cfg.entities["merchants"]
    q = evaluate(e, bronze_rows(e, [merchant(updated_at="last tuesday")] + [merchant(f"M{i}") for i in range(30)]),
                 {}, {}, "b")
    assert q.quarantine["reasons"].tolist() == ["type_cast:updated_at"]


def test_malformed_records(cfg):
    e = cfg.entities["merchants"]
    rows = [merchant(f"M{i}") for i in range(30)] + [{"_corrupt_record": "{oops"}]
    q = evaluate(e, bronze_rows(e, rows), {}, {}, "b")
    assert q.quarantine["reasons"].tolist() == ["malformed_record"]
    assert "{oops" in q.quarantine["record"].iloc[0]


def test_schema_drift_rejects_the_whole_file_and_blocks_the_batch(cfg):
    e = cfg.entities["merchants"]
    df = bronze_rows(e, [merchant(f"M{i}", category=None) for i in range(3)], source="drift.csv")
    q = evaluate(e, df, {"drift.csv": {"missing_columns": "category"}}, {}, "b")
    assert q.quarantine["reasons"].unique().tolist() == ["schema_drift:category"]
    assert q.blocked and q.valid.empty
    assert q.failure_rate == 1.0


def test_gate_blocks_above_threshold(cfg):
    e = cfg.entities["merchants"]       # max_failure_rate 5%
    good = [merchant(f"M{i}") for i in range(19)]
    bad = [merchant(f"N{i}", merchant_id=None, merchant_name=f"Shop {i}") for i in range(2)]
    rows = good[:18] + bad
    q = evaluate(e, bronze_rows(e, rows), {}, {}, "b")
    assert q.failure_rate == pytest.approx(0.10)
    assert q.blocked
    assert {r["gate_status"] for r in q.results} == {"BLOCKED"}
    q_ok = evaluate(e, bronze_rows(e, good + bad[:1]), {}, {}, "b")    # exactly 5% is allowed
    assert not q_ok.blocked


def test_latest_per_key_keeps_newest_version():
    df = pd.DataFrame({"k": ["a", "a", "b"],
                       "ts": pd.to_datetime(["2026-11-01T02:00Z", "2026-11-01T01:00Z", "2026-11-01T00:00Z"]),
                       "_line_no": [1, 2, 3], "v": ["new", "old", "b"]})
    out, superseded = latest_per_key(df, "k", "ts")
    assert out["v"].tolist() == ["new", "b"] and superseded == 1
