import pytest
import yaml

from lakehouse.config import DEFAULT_CONFIG, load_settings


def test_default_config_loads():
    cfg = load_settings()
    assert set(cfg.entities) == {"customers", "merchants", "transactions"}
    assert cfg.entities["customers"].write_mode == "scd2"
    assert cfg.pii.for_entity("customers")["email"] == "hash"


def test_processing_order_puts_referenced_entities_first():
    cfg = load_settings()
    order = cfg.order
    assert order.index("customers") < order.index("transactions")
    assert order.index("merchants") < order.index("transactions")


@pytest.mark.parametrize("mutate, message", [
    (lambda r: r["entities"]["customers"]["rules"].append({"name": "x", "type": "nope", "column": "email"}),
     "unknown rule type"),
    (lambda r: r["entities"]["customers"]["rules"].append({"name": "x", "type": "not_null", "column": "missing"}),
     "unknown column"),
    (lambda r: r["entities"]["transactions"]["rules"].append(
        {"name": "x", "type": "foreign_key", "column": "txn_id", "references": "accounts.id"}), "unknown entity"),
    (lambda r: r["pii"]["columns"].update({"customers.city": {"method": "scramble"}}), "unknown masking method"),
])
def test_invalid_config_is_rejected(tmp_path, mutate, message):
    raw = yaml.safe_load(DEFAULT_CONFIG.read_text())
    mutate(raw)
    path = tmp_path / "bad.yaml"
    path.write_text(yaml.safe_dump(raw))
    with pytest.raises(ValueError, match=message):
        load_settings(path)


def test_salt_comes_from_the_environment(monkeypatch):
    cfg = load_settings()
    assert cfg.pii.salt() == cfg.pii.demo_salt
    monkeypatch.setenv(cfg.pii.salt_env, "from-key-vault")
    assert cfg.pii.salt() == "from-key-vault"
