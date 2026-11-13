import json

from click.testing import CliRunner

from lakehouse.cli import cli
from tests.conftest import small_config


def test_cli_end_to_end(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    path = small_config(tmp_path, days=3)
    # load_settings resolves paths against the repo root by default; point them at tmp
    raw = path.read_text().replace("data/", f"{tmp_path}/data/").replace("docs/images", f"{tmp_path}/img")
    path.write_text(raw)
    r = CliRunner()
    for args in (["generate"], ["run"], ["report"], ["uc-sql", "--out", str(tmp_path / "uc.sql")]):
        res = r.invoke(cli, ["--config", str(path), *args])
        assert res.exit_code == 0, res.output
    res = r.invoke(cli, ["--config", str(path), "summary"])
    s = json.loads(res.output)
    assert s["batches"] == 3 and s["landing_files"] == 9
    assert s["blocked_batches"] == ["2026-11-03 merchants"]
    assert len(list((tmp_path / "img").glob("*.png"))) == 6
    res = r.invoke(cli, ["--config", str(path), "history", "silver.customers"])
    assert "MERGE" in res.output
