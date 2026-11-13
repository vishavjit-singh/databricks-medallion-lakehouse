from datetime import date

import pandas as pd
import pytest

from lakehouse.storage import TableStore


@pytest.mark.parametrize("fmt", ["csv"])
def test_versions_append_overwrite_and_time_travel(tmp_path, fmt):
    s = TableStore(tmp_path, fmt)
    v0 = s.write("silver", "t", pd.DataFrame({"id": ["a"], "n": [1]}), mode="append")
    v1 = s.write("silver", "t", pd.DataFrame({"id": ["b"], "n": [2]}), mode="append")
    v2 = s.write("silver", "t", pd.DataFrame({"id": ["c"], "n": [3]}), mode="overwrite", operation="MERGE")
    assert (v0, v1, v2) == (0, 1, 2)
    assert list(s.read("silver", "t")["id"]) == ["c"]
    assert sorted(s.read("silver", "t", version=1)["id"]) == ["a", "b"]
    assert [c["operation"] for c in s.history("silver", "t")] == ["WRITE", "WRITE", "MERGE"]


def test_types_survive_a_csv_round_trip(tmp_path):
    s = TableStore(tmp_path, "csv")
    df = pd.DataFrame({
        "ts": pd.to_datetime(["2026-11-01T10:00:00Z", None], utc=True),
        "d": [date(2026, 11, 1), None],
        "amount": [10.25, None],
        "flag": [True, False],
        "s": ["x", None],
    })
    s.write("gold", "t", df, mode="overwrite")
    back = s.read("gold", "t")
    assert str(back["ts"].dtype).startswith("datetime64") and back["ts"].dt.tz is not None
    assert back["d"].iloc[0] == date(2026, 11, 1) and back["d"].iloc[1] is None
    assert back["amount"].iloc[0] == 10.25
    assert back["flag"].tolist() == [True, False]
    assert back["s"].iloc[1] is None


def test_commit_is_exclusive(tmp_path, monkeypatch):
    """Two writers read version 0, both try to commit version 1: the second one must fail, not overwrite."""
    s = TableStore(tmp_path, "csv")
    s.write("bronze", "t", pd.DataFrame({"a": [1]}))
    stale = s.history("bronze", "t")
    TableStore(tmp_path, "csv").write("bronze", "t", pd.DataFrame({"a": [2]}))   # the other writer wins v1
    monkeypatch.setattr(s, "history", lambda layer, name: stale)
    with pytest.raises(FileExistsError):
        s.write("bronze", "t", pd.DataFrame({"a": [3]}))


def test_missing_table(tmp_path):
    s = TableStore(tmp_path)
    assert not s.exists("gold", "nope")
    with pytest.raises(FileNotFoundError):
        s.read("gold", "nope")
    assert list(s.read_or_empty("gold", "nope", ["x"]).columns) == ["x"]
