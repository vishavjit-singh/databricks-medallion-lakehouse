"""Typed view of ``config/pipeline.yaml``, shared by the pandas and Spark engines."""
from __future__ import annotations

import os
from dataclasses import dataclass, field
from datetime import date
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[2]
DEFAULT_CONFIG = ROOT / "config" / "pipeline.yaml"
SUPPORTED_TYPES = {"string", "date", "timestamp", "decimal"}
RULE_TYPES = {"not_null", "regex", "accepted_values", "range", "foreign_key"}
MASK_METHODS = {"redact", "hash", "mask_last4", "year_only"}


@dataclass(frozen=True)
class Rule:
    name: str
    type: str
    column: str
    pattern: str | None = None
    values: tuple[str, ...] = ()
    min: float | None = None
    max: float | None = None
    references: str | None = None   # "<entity>.<column>"

    @property
    def ref_entity(self) -> str:
        return self.references.split(".", 1)[0]

    @property
    def ref_column(self) -> str:
        return self.references.split(".", 1)[1]


@dataclass(frozen=True)
class Entity:
    name: str
    format: str
    primary_key: str
    order_by: str
    write_mode: str
    schema: dict[str, str]
    rules: tuple[Rule, ...]
    max_failure_rate: float
    tracked_columns: tuple[str, ...] = ()

    @property
    def columns(self) -> list[str]:
        return list(self.schema)


@dataclass(frozen=True)
class PiiPolicy:
    salt_env: str
    demo_salt: str
    reader_group: str
    columns: dict[str, str]          # "entity.column" -> method

    def salt(self) -> str:
        return os.environ.get(self.salt_env, self.demo_salt)

    def for_entity(self, entity: str) -> dict[str, str]:
        prefix = f"{entity}."
        return {k[len(prefix):]: v for k, v in self.columns.items() if k.startswith(prefix)}


@dataclass(frozen=True)
class GeneratorSettings:
    seed: int
    start_date: date
    days: int
    customers: int
    merchants: int
    transactions_per_day: int


@dataclass
class Settings:
    root: Path
    paths: dict[str, str]
    generator: GeneratorSettings
    fx_to_inr: dict[str, float]
    entities: dict[str, Entity]
    pii: PiiPolicy
    raw: dict = field(repr=False, default_factory=dict)

    def path(self, key: str) -> Path:
        p = Path(self.paths[key])
        return p if p.is_absolute() else self.root / p

    @property
    def order(self) -> list[str]:
        """Processing order: referenced entities before the ones that reference them."""
        done: list[str] = []

        def visit(name: str, stack: tuple[str, ...] = ()) -> None:
            if name in done:
                return
            if name in stack:
                raise ValueError(f"foreign-key cycle through {name}")
            for rule in self.entities[name].rules:
                if rule.type == "foreign_key":
                    visit(rule.ref_entity, stack + (name,))
            done.append(name)

        for name in self.entities:
            visit(name)
        return done


def _rule(d: dict) -> Rule:
    if d["type"] not in RULE_TYPES:
        raise ValueError(f"unknown rule type {d['type']!r} in {d['name']}")
    return Rule(name=d["name"], type=d["type"], column=d["column"], pattern=d.get("pattern"),
                values=tuple(d.get("values", ())), min=d.get("min"), max=d.get("max"),
                references=d.get("references"))


def load_settings(path: str | Path | None = None, root: Path | None = None) -> Settings:
    path = Path(path) if path else DEFAULT_CONFIG
    raw = yaml.safe_load(path.read_text())
    entities = {}
    for name, e in raw["entities"].items():
        bad = set(e["schema"].values()) - SUPPORTED_TYPES
        if bad:
            raise ValueError(f"{name}: unsupported types {bad}")
        rules = tuple(_rule(r) for r in e.get("rules", ()))
        for r in rules:
            if r.column not in e["schema"]:
                raise ValueError(f"{name}.{r.name}: unknown column {r.column}")
        entities[name] = Entity(name=name, format=e["format"], primary_key=e["primary_key"],
                                order_by=e["order_by"], write_mode=e["write_mode"], schema=dict(e["schema"]),
                                rules=rules, max_failure_rate=float(e["gate"]["max_failure_rate"]),
                                tracked_columns=tuple(e.get("tracked_columns", ())))
    for e in entities.values():
        for r in e.rules:
            if r.type == "foreign_key" and r.ref_entity not in entities:
                raise ValueError(f"{e.name}.{r.name} references unknown entity {r.ref_entity}")
    p = raw["pii"]
    for col, spec in p["columns"].items():
        if spec["method"] not in MASK_METHODS:
            raise ValueError(f"{col}: unknown masking method {spec['method']}")
    pii = PiiPolicy(salt_env=p["salt_env"], demo_salt=p["demo_salt"], reader_group=p["reader_group"],
                    columns={k: v["method"] for k, v in p["columns"].items()})
    g = raw["generator"]
    gen = GeneratorSettings(seed=int(g["seed"]), start_date=date.fromisoformat(str(g["start_date"])),
                            days=int(g["days"]), customers=int(g["customers"]), merchants=int(g["merchants"]),
                            transactions_per_day=int(g["transactions_per_day"]))
    return Settings(root=root or ROOT, paths=raw["paths"], generator=gen,
                    fx_to_inr={k: float(v) for k, v in raw["fx_to_inr"].items()},
                    entities=entities, pii=pii, raw=raw)
