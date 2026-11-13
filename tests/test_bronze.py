import json

from lakehouse import bronze
from lakehouse.generator import batch_dates, generate
from lakehouse.storage import TableStore


def test_csv_extra_and_missing_columns(cfg, tmp_path):
    f = tmp_path / "m.csv"
    f.write_text("merchant_id,merchant_name,merchant_category,city,updated_at\nM1,Shop,food,Delhi,2026-11-01T00:00:00Z\n")
    rows, info = bronze.parse_file(f, cfg.entities["merchants"])
    assert info["missing_columns"] == "category"
    assert info["extra_columns"] == "merchant_category"
    assert rows[0]["category"] is None
    assert json.loads(rows[0]["_rescued_data"]) == {"merchant_category": "food"}
    assert rows[0]["_line_no"] == 2


def test_jsonl_corrupt_lines_are_kept(cfg, tmp_path):
    f = tmp_path / "t.jsonl"
    f.write_text('{"txn_id": "T1", "amount": 10.5}\nnot json\n[1, 2]\n')
    rows, info = bronze.parse_file(f, cfg.entities["transactions"])
    assert len(rows) == 3
    assert info["corrupt_rows"] == 2
    assert rows[0]["amount"] == "10.5" and rows[0]["_corrupt_record"] is None
    assert rows[1]["_corrupt_record"] == "not json"
    assert info["missing_columns"]  # a line with only two fields does not satisfy the contract


def test_ingest_is_idempotent(cfg):
    generate(cfg)
    store = TableStore(cfg.path("lake"))
    day = batch_dates(cfg)[0]
    first = bronze.ingest(cfg, store, day)
    rows = len(store.read("bronze", "transactions"))
    second = bronze.ingest(cfg, store, day)
    assert first["transactions"]["files"] == 1 and first["transactions"]["rows"] == rows
    assert second["transactions"] == {"files": 0, "files_skipped": 1, "rows": 0}
    assert len(store.read("bronze", "transactions")) == rows
    assert len(store.read("bronze", bronze.MANIFEST)) == 3


def test_redelivered_file_with_new_content_is_ingested(cfg):
    generate(cfg)
    store = TableStore(cfg.path("lake"))
    day = batch_dates(cfg)[0]
    bronze.ingest(cfg, store, day)
    path = cfg.path("landing") / "merchants" / day / f"merchants_{day}.csv"
    path.write_text(path.read_text() + "M9999,Late Shop,grocery,Delhi,2026-11-01T10:00:00Z\n")
    assert bronze.ingest(cfg, store, day)["merchants"]["files"] == 1
