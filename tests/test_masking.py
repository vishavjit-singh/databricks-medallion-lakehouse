from datetime import date

import pandas as pd

from lakehouse.config import load_settings
from lakehouse.masking import (apply_policy, hash_value, mask_last4, raw_pii_values, scan_for_leaks,
                               unity_catalog_sql, year_only)


def test_methods():
    assert hash_value(" A@Example.com ", "s") == hash_value("a@example.com", "s")
    assert hash_value("a@example.com", "s") != hash_value("a@example.com", "t")
    assert len(hash_value("x", "s")) == 64 and hash_value(None, "s") is None
    assert mask_last4("+91 9876543210") == "**********3210"
    assert mask_last4("12") == "12"
    assert year_only(date(1994, 5, 2)) == 1994 and year_only("1994-05-02") == 1994


def test_policy_applies_every_column():
    cfg = load_settings()
    df = pd.DataFrame([{"full_name": "Asha Rao", "email": "asha@example.com", "phone": "+91 9876543210",
                        "date_of_birth": date(1994, 5, 2), "city": "Pune"}])
    out = apply_policy(df, cfg.pii.for_entity("customers"), "salt")
    assert out.loc[0, "full_name"] == "***"
    assert out.loc[0, "email"] == hash_value("asha@example.com", "salt")
    assert out.loc[0, "phone"].endswith("3210") and "9876" not in out.loc[0, "phone"]
    assert out.loc[0, "date_of_birth_year"] == 1994 and "date_of_birth" not in out
    assert out.loc[0, "city"] == "Pune"


def test_leak_scan_finds_raw_values_and_patterns():
    raw = {"Asha Rao", "asha@example.com"}
    clean = pd.DataFrame({"name": ["***"], "d": ["2026-11-01"], "amount": [10.0]})
    assert scan_for_leaks({"t": clean}, raw) == []
    leaky = pd.DataFrame({"note": ["call +91 9876543210"], "who": ["Asha Rao"], "mail": ["x@y.com"]})
    kinds = {f["column"]: f["kind"] for f in scan_for_leaks({"t": leaky}, raw)}
    assert kinds == {"who": "raw_value", "mail": "email_pattern"}   # a phone inside free text is not matched
    assert scan_for_leaks({"t": pd.DataFrame({"p": ["+91 9876543210"]})}, set())[0]["kind"] == "phone_pattern"


def test_raw_values_skip_dates():
    cfg = load_settings()
    df = pd.DataFrame({"full_name": ["A B"], "email": ["a@b.com"], "phone": ["1"], "date_of_birth": ["1990-01-01"]})
    assert raw_pii_values(df, cfg.pii) == {"A B", "a@b.com", "1"}


def test_unity_catalog_sql():
    sql = unity_catalog_sql(load_settings().pii, catalog="dev")
    assert "USE CATALOG dev;" in sql
    assert "ALTER TABLE customers ALTER COLUMN email SET MASK pii_mask_hash;" in sql
    assert "is_account_group_member('pii_readers')" in sql
    assert sql.count("CREATE OR REPLACE FUNCTION") == 4
