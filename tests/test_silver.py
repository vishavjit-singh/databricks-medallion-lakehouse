import pandas as pd

from lakehouse.silver import scd2_merge, upsert


def ts(s):
    return pd.Timestamp(s, tz="UTC")


def cust(cid, city, tier, updated):
    return {"customer_id": cid, "email": f"{cid}@example.com", "phone": "+91 9000000000", "city": city,
            "kyc_tier": tier, "updated_at": ts(updated)}


def test_scd2_insert_change_unchanged_and_stale(cfg):
    e = cfg.entities["customers"]
    day1 = pd.DataFrame([cust("C1", "Delhi", "basic", "2026-11-01"), cust("C2", "Pune", "basic", "2026-11-01")])
    s1, m1 = scd2_merge(pd.DataFrame(), day1, e)
    assert m1["inserted"] == 2 and s1["is_current"].all()

    day2 = pd.DataFrame([
        cust("C1", "Mohali", "basic", "2026-11-02"),     # tracked change → new version
        cust("C2", "Pune", "basic", "2026-11-02"),       # nothing changed
        cust("C3", "Jaipur", "premium", "2026-11-02"),   # new customer
    ])
    s2, m2 = scd2_merge(s1, day2, e)
    assert m2 == {"inserted": 1, "updated": 1, "unchanged": 1, "stale": 0}
    c1 = s2[s2["customer_id"] == "C1"].sort_values("valid_from")
    assert c1["city"].tolist() == ["Delhi", "Mohali"]
    assert c1["is_current"].tolist() == [False, True]
    assert c1["valid_to"].iloc[0] == ts("2026-11-02") and pd.isna(c1["valid_to"].iloc[1])

    late = pd.DataFrame([cust("C1", "Patiala", "basic", "2026-11-01T12:00")])   # older than current version
    s3, m3 = scd2_merge(s2, late, e)
    assert m3["stale"] == 1 and len(s3) == len(s2)

    again, m4 = scd2_merge(s2, day2, e)        # replaying the same batch changes nothing
    assert m4["inserted"] == 0 and m4["updated"] == 0
    assert len(again) == len(s2)
    assert again.groupby("customer_id")["is_current"].sum().eq(1).all()


def test_upsert_newest_wins(cfg):
    e = cfg.entities["transactions"]
    t1 = pd.DataFrame([{"txn_id": "T1", "status": "captured", "updated_at": ts("2026-11-01")},
                       {"txn_id": "T2", "status": "captured", "updated_at": ts("2026-11-01")}])
    s1, _ = upsert(pd.DataFrame(), t1, e)
    t2 = pd.DataFrame([{"txn_id": "T1", "status": "refunded", "updated_at": ts("2026-11-03")},
                       {"txn_id": "T2", "status": "failed", "updated_at": ts("2026-10-30")},
                       {"txn_id": "T3", "status": "captured", "updated_at": ts("2026-11-03")}])
    s2, m = upsert(s1, t2, e)
    assert m == {"inserted": 1, "updated": 1, "unchanged": 0, "stale": 1}
    assert dict(zip(s2["txn_id"], s2["status"])) == {"T1": "refunded", "T2": "captured", "T3": "captured"}
