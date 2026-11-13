"""Gold: consumer-facing marts. Rebuilt from silver on every run, so they are always consistent with it.

* ``daily_merchant_revenue``: one row per transaction date and merchant category.
* ``dim_customer``: current customer version with the PII policy applied, plus lifetime metrics.
* ``dq_scorecard``: pass rate per rule, entity and batch, with the gate decision.

No gold table contains raw PII; the pipeline scans them after every build and fails if one does.
"""
from __future__ import annotations

import pandas as pd

from .config import Settings
from .masking import apply_policy

PAYMENT_STATUSES = ("captured", "refunded")
DIM_CUSTOMER_COLUMNS = ["customer_id", "full_name", "email", "phone", "city", "country", "kyc_tier", "signup_date",
                        "current_since", "payments", "net_spend_inr", "first_txn_date", "last_txn_date", "versions",
                        "date_of_birth_year"]
REVENUE_COLUMNS = ["txn_date", "category", "payments", "failed", "refunds", "gross_inr", "refunded_inr",
                   "paying_customers", "net_inr"]


def daily_merchant_revenue(transactions: pd.DataFrame, merchants: pd.DataFrame) -> pd.DataFrame:
    t = transactions.merge(merchants[["merchant_id", "category"]], on="merchant_id", how="left")
    t["txn_date"] = t["txn_ts"].dt.strftime("%Y-%m-%d")
    t["category"] = t["category"].fillna("unknown")
    paid = t["status"].isin(PAYMENT_STATUSES)
    refunded = t["status"] == "refunded"
    t = t.assign(
        is_payment=paid.astype(int),
        is_refund=refunded.astype(int),
        is_failed=(t["status"] == "failed").astype(int),
        gross=t["amount_inr"].where(paid, 0.0),
        refund_amt=t["amount_inr"].where(refunded, 0.0),
        payer=t["customer_id"].where(paid),
    )
    g = t.groupby(["txn_date", "category"], as_index=False).agg(
        payments=("is_payment", "sum"),
        failed=("is_failed", "sum"),
        refunds=("is_refund", "sum"),
        gross_inr=("gross", "sum"),
        refunded_inr=("refund_amt", "sum"),
        paying_customers=("payer", "nunique"),
    )
    g["net_inr"] = g["gross_inr"] - g["refunded_inr"]
    for c in ("gross_inr", "refunded_inr", "net_inr"):
        g[c] = g[c].round(2)
    for c in ("payments", "failed", "refunds", "paying_customers"):
        g[c] = g[c].astype("int64")
    return g[REVENUE_COLUMNS].sort_values(["txn_date", "category"]).reset_index(drop=True)


def dim_customer(cfg: Settings, customers: pd.DataFrame, transactions: pd.DataFrame) -> pd.DataFrame:
    cur = customers[customers["is_current"].astype(bool)].copy()
    paid = transactions[transactions["status"].isin(PAYMENT_STATUSES)].copy()
    paid["net"] = paid["amount_inr"].where(paid["status"] == "captured", 0.0)
    paid["txn_date"] = paid["txn_ts"].dt.strftime("%Y-%m-%d")
    stats = paid.groupby("customer_id", as_index=False).agg(
        payments=("txn_id", "count"), net_spend_inr=("net", "sum"),
        first_txn_date=("txn_date", "min"), last_txn_date=("txn_date", "max"))
    versions = customers.groupby("customer_id", as_index=False).size().rename(columns={"size": "versions"})
    cols = ["customer_id", "full_name", "email", "phone", "date_of_birth", "city", "country", "kyc_tier",
            "signup_date", "valid_from"]
    out = cur[cols].merge(stats, on="customer_id", how="left").merge(versions, on="customer_id", how="left")
    out["payments"] = out["payments"].fillna(0).astype("int64")
    out["net_spend_inr"] = out["net_spend_inr"].fillna(0.0).round(2)
    out["versions"] = out["versions"].astype("int64")
    out["signup_date"] = out["signup_date"].map(lambda d: d.isoformat() if d is not None and d == d else None)
    out = out.rename(columns={"valid_from": "current_since"})
    out = apply_policy(out, cfg.pii.for_entity("customers"), cfg.pii.salt())
    return out[DIM_CUSTOMER_COLUMNS].sort_values("customer_id").reset_index(drop=True)


def dq_scorecard(dq_results: pd.DataFrame) -> pd.DataFrame:
    s = dq_results.copy()
    s["checked"] = s["checked"].astype("int64")
    s["failed"] = s["failed"].astype("int64")
    s["pass_rate"] = (1 - s["failed"] / s["checked"].where(s["checked"] > 0)).fillna(1.0).round(4)
    cols = ["batch_id", "entity", "rule", "rule_type", "column", "checked", "failed", "pass_rate", "gate_status"]
    return s[cols].sort_values(["batch_id", "entity", "rule"]).reset_index(drop=True)
