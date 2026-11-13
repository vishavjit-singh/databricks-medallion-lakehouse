"""Render the README images from the lake tables and the per-batch run summaries.

Every number in these charts is computed from what the pipeline actually wrote.
"""
from __future__ import annotations

from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
import numpy as np  # noqa: E402
import pandas as pd  # noqa: E402
from matplotlib.colors import LinearSegmentedColormap  # noqa: E402
from matplotlib.patches import FancyArrowPatch, FancyBboxPatch  # noqa: E402

from .config import Settings  # noqa: E402
from .pipeline import load_runs  # noqa: E402
from .storage import TableStore  # noqa: E402

INK = "#0e1a2b"
MUTED = "#5b6676"
GRID = "#e3e7ee"
ACCENT = "#2a78d6"
SIGNAL = "#eda100"
BAD = "#e34948"
GOOD = "#1baf7a"
BRONZE = "#b0702b"
SILVER = "#7d8794"
GOLD = "#c99a06"
CATEGORY_COLORS = ["#2a78d6", "#eb6834", "#1baf7a", "#6250d6", "#eda100", "#d6458f", "#2fa8b8", "#8a6a3b"]

plt.rcParams.update({
    "font.family": "DejaVu Sans",
    "axes.edgecolor": GRID,
    "axes.labelcolor": MUTED,
    "xtick.color": MUTED,
    "ytick.color": MUTED,
    "axes.titleweight": "bold",
    "axes.titlesize": 13,
    "axes.titlecolor": INK,
    "axes.titlelocation": "left",
    "axes.spines.top": False,
    "axes.spines.right": False,
    "figure.dpi": 150,
})


def _inr(x: float, _pos=None) -> str:
    if abs(x) >= 1e5:
        return f"₹{x / 1e5:.1f} L"
    if abs(x) >= 1e3:
        return f"₹{x / 1e3:.0f}k"
    return f"₹{x:,.0f}"


def _save(fig, out: Path, name: str) -> Path:
    fig.tight_layout()
    path = out / name
    fig.savefig(path, facecolor="white")
    plt.close(fig)
    return path


# ------------------------------------------------------------------------------------------- numbers
def summary(cfg: Settings, store: TableStore) -> dict:
    runs = load_runs(store.root)
    manifest = store.read("bronze", "_ingest_manifest")
    q = store.read("silver", "_quarantine")
    customers = store.read("silver", "customers")
    ent = {name: {k: int(sum(r["entities"][name].get(k, 0) for r in runs))
                  for k in ("bronze_batch_rows", "duplicates_dropped", "quarantined", "inserted", "updated", "stale")}
           for name in cfg.order}
    for name in cfg.order:
        ent[name]["silver_rows"] = len(store.read("silver", name))
    blocked = [f"{r['batch_id']} {n}" for r in runs for n, e in r["entities"].items() if e.get("blocked")]
    reasons = q["reasons"].str.split(";").explode().str.split(":").str[0].value_counts()
    return {
        "batches": len(runs),
        "landing_files": int(len(manifest)),
        "entities": ent,
        "blocked_batches": blocked,
        "quarantine_rows": int(len(q)),
        "quarantine_reasons": {k: int(v) for k, v in reasons.items()},
        "rescued_files": int((manifest["extra_columns"].fillna("") != "").sum()),
        "customers_with_history": int((customers.groupby("customer_id").size() > 1).sum()),
        "customer_versions": int(len(customers)),
        "gold_rows": {n: len(store.read("gold", n))
                      for n in ("daily_merchant_revenue", "dim_customer", "dq_scorecard")},
        "dq_rules_evaluated": int(store.read("gold", "dq_scorecard")["rule"].nunique()),
        "pii_leak_findings": 0,
    }


# ------------------------------------------------------------------------------------------- charts
def architecture(out: Path) -> Path:
    fig, ax = plt.subplots(figsize=(13, 6.0))
    ax.set_xlim(0, 13)
    ax.set_ylim(0, 6.4)
    ax.axis("off")

    def box(x, y, w, h, title, sub, color=INK, lw=2):
        ax.add_patch(FancyBboxPatch((x, y), w, h, boxstyle="round,pad=0.02,rounding_size=0.12",
                                    fc="white", ec=color, lw=lw))
        ax.text(x + w / 2, y + h * 0.7, title, ha="center", va="center", fontsize=11, weight="bold", color=color)
        ax.text(x + w / 2, y + h * 0.32, sub, ha="center", va="center", fontsize=8.2, color=MUTED, linespacing=1.4)

    def arrow(x1, y1, x2, y2, color=MUTED, label=None):
        ax.add_patch(FancyArrowPatch((x1, y1), (x2, y2), arrowstyle="-|>", mutation_scale=14, lw=1.6, color=color))
        if label:
            ax.text((x1 + x2) / 2, (y1 + y2) / 2 + 0.13, label, ha="center", fontsize=7.6, color=color)

    ax.text(0.15, 6.12, "databricks-medallion-lakehouse: bronze → silver → gold with quality gates and PII masking",
            fontsize=13.5, weight="bold", color=INK)
    box(0.2, 2.75, 2.0, 1.45, "Landing zone", "daily drops (ADLS / Volume)\ncustomers.csv · merchants.csv\n"
        "transactions.jsonl")
    box(2.75, 2.75, 2.1, 1.45, "Bronze", "raw strings + lineage\n_rescued_data · _corrupt_record\n"
        "SHA-256 ingest manifest", BRONZE)
    box(5.45, 3.55, 2.25, 1.35, "Quality gates", "1 schema contract (file)\n2 casts + rules (row)\n"
        "3 failure-rate gate (batch)", SIGNAL)
    box(5.45, 1.75, 2.25, 1.2, "_quarantine", "every failed row\nwith all its reasons", BAD)
    box(5.45, 0.2, 2.25, 1.05, "dq_scorecard (gold)", "pass rate per rule,\nentity and batch", GOLD)
    box(8.3, 3.55, 2.1, 1.35, "Silver (restricted)", "typed · deduped · conformed\nMERGE: SCD2 customers\n"
        "upsert txns / merchants", SILVER)
    box(10.85, 4.35, 2.0, 1.25, "Gold", "daily_merchant_revenue\ndim_customer (masked)", GOLD)
    box(8.3, 1.55, 2.1, 1.3, "PII policy", "redact · hash · mask_last4\nyear_only  → gold\n"
        "same YAML → UC masks", ACCENT)
    box(10.85, 1.55, 2.0, 1.3, "PII leak scan", "raw values · emails · phones\nfail → nothing published", BAD)

    arrow(2.2, 3.47, 2.75, 3.47)
    arrow(4.85, 3.7, 5.45, 4.1)
    arrow(6.57, 3.55, 6.57, 2.95, BAD)
    ax.text(6.67, 3.18, "fail", fontsize=7.6, color=BAD)
    arrow(6.57, 1.75, 6.57, 1.25, MUTED)
    ax.text(6.67, 1.43, "+ rule results", fontsize=7.6, color=MUTED)
    arrow(7.7, 4.22, 8.3, 4.22, GOOD, "pass")
    arrow(10.4, 4.5, 10.85, 4.85)
    arrow(9.35, 2.85, 9.35, 3.55, ACCENT)
    arrow(10.4, 2.2, 10.85, 2.2)
    arrow(11.85, 2.85, 11.85, 4.35, BAD)
    ax.text(11.95, 3.55, "checked before\nevery publish", fontsize=7.6, color=BAD, va="center")
    ax.text(0.2, 2.35, "Two engines, one config/pipeline.yaml\n• PySpark + Delta Lake: MERGE, replaceWhere,\n"
            "  Unity Catalog names, Databricks notebooks/job\n• pandas reference engine: same rules,\n"
            "  runs anywhere (make demo); parity-tested", fontsize=8.4, color=MUTED, va="top", linespacing=1.45)
    ax.text(0.2, 0.75, "Blocked batch → nothing published to silver;\nrows stay in bronze + quarantine for replay.",
            fontsize=8.2, color="#9a6a00", va="top")
    return _save(fig, out, "architecture.png")


def layer_flow(runs: list[dict], out: Path) -> Path:
    entities = list(runs[0]["entities"])
    fig, axes = plt.subplots(1, len(entities), figsize=(13, 4.2), sharey=False)
    for ax, name in zip(axes, entities):
        dates = [r["batch_id"][5:] for r in runs]
        e = [r["entities"][name] for r in runs]
        dups = np.array([x.get("duplicates_dropped", 0) for x in e])
        quar = np.array([x.get("quarantined", 0) for x in e])
        checked = np.array([x.get("checked", 0) for x in e])
        blocked = np.array([bool(x.get("blocked")) for x in e])
        held = np.where(blocked, checked - quar, 0)
        passed = np.where(blocked, 0, checked - quar)
        x = np.arange(len(dates))
        ax.bar(x, passed, color=GOOD, label="passed → silver MERGE", width=0.65)
        ax.bar(x, held, bottom=passed, color=SIGNAL, label="held: batch blocked by gate", width=0.65)
        ax.bar(x, quar, bottom=passed + held, color=BAD, label="quarantined", width=0.65)
        ax.bar(x, dups, bottom=passed + held + quar, color=GRID, label="exact duplicate dropped", width=0.65)
        top = passed + held + quar + dups
        for i in range(len(x)):
            ax.text(x[i], top[i] * 1.01, "BLOCKED" if blocked[i] else f"{quar[i]}", ha="center", va="bottom",
                    fontsize=7.5, color=SIGNAL if blocked[i] else BAD, weight="bold" if blocked[i] else None)
        ax.set_xticks(x, dates)
        ax.set_title(name, fontsize=11.5)
        ax.grid(axis="y", color=GRID)
        ax.set_ylim(0, top.max() * 1.15)
    axes[0].set_ylabel("bronze rows in the batch")
    handles, labels = axes[0].get_legend_handles_labels()
    fig.legend(handles, labels, loc="lower center", ncol=4, frameon=False, fontsize=9)
    fig.suptitle("Where every bronze row went, per daily batch (label = rows quarantined)", x=0.01, ha="left",
                 fontsize=13, weight="bold", color=INK)
    fig.tight_layout(rect=(0, 0.08, 1, 0.97))
    path = out / "layer_flow.png"
    fig.savefig(path, facecolor="white")
    plt.close(fig)
    return path


def scorecard(sc: pd.DataFrame, out: Path) -> Path:
    sc = sc[sc["rule_type"] != "gate"].copy()
    sc["label"] = sc["entity"] + " · " + sc["rule"]
    order = list(dict.fromkeys(sc.sort_values(["entity"], kind="stable")["label"]))
    piv = sc.pivot_table(index="label", columns="batch_id", values="pass_rate", aggfunc="first").reindex(order)
    fails = sc.pivot_table(index="label", columns="batch_id", values="failed", aggfunc="first").reindex(order)
    cmap = LinearSegmentedColormap.from_list("dq", [BAD, "#f6c26b", "#e8f6ef", GOOD], N=256)
    fig, ax = plt.subplots(figsize=(11, 0.34 * len(order) + 1.4))
    ax.imshow(piv.values, cmap=cmap, vmin=0.85, vmax=1.0, aspect="auto")
    for i in range(piv.shape[0]):
        for j in range(piv.shape[1]):
            v, f = piv.values[i, j], fails.values[i, j]
            if np.isnan(v):
                continue
            txt = "✓" if f == 0 else f"{int(f)} ({v:.1%})"
            ax.text(j, i, txt, ha="center", va="center", fontsize=8, color=INK if v > 0.9 else "white")
    ax.set_yticks(range(len(order)), order, fontsize=8.5)
    ax.set_xticks(range(piv.shape[1]), [c[5:] for c in piv.columns])
    ax.tick_params(length=0)
    for s in ax.spines.values():
        s.set_visible(False)
    ax.set_title("gold.dq_scorecard: failed rows (pass rate) per rule and batch; ✓ = no failures")
    return _save(fig, out, "dq_scorecard.png")


def quarantine_reasons(q: pd.DataFrame, out: Path) -> Path:
    r = q.assign(reason=q["reasons"].str.split(";")).explode("reason")
    r["reason"] = r["entity"] + " · " + r["reason"].str.split(":").str[0]
    counts = r["reason"].value_counts().sort_values()
    fig, ax = plt.subplots(figsize=(10, 0.32 * len(counts) + 1.4))
    ax.barh(counts.index, counts.values, color=BAD, height=0.6)
    for i, v in enumerate(counts.values):
        ax.text(v + counts.max() * 0.01, i, f" {v}", va="center", fontsize=9, color=INK)
    ax.set_xlim(0, counts.max() * 1.15)
    ax.grid(axis="x", color=GRID)
    ax.set_title(f"_quarantine: rows per failed rule ({len(q):,} rows; one row can fail several)")
    ax.set_xlabel("rows")
    return _save(fig, out, "quarantine_reasons.png")


def revenue(rev: pd.DataFrame, runs: list[dict], out: Path) -> Path:
    piv = rev.pivot_table(index="txn_date", columns="category", values="net_inr", aggfunc="sum").fillna(0)
    fig, ax = plt.subplots(figsize=(11, 4.4))
    bottom = np.zeros(len(piv))
    x = np.arange(len(piv))
    for i, cat in enumerate(piv.columns):
        ax.bar(x, piv[cat].values, bottom=bottom, color=CATEGORY_COLORS[i % 8], label=cat, width=0.62)
        bottom += piv[cat].values
    blocked = {r["batch_id"] for r in runs if r["entities"]["transactions"].get("blocked")}
    for i, d in enumerate(piv.index):
        if d in blocked:
            ax.annotate("transactions batch\nblocked by the quality\ngate (lower-case\ncurrencies): held in\n"
                        "bronze, not published", xy=(i, bottom[i]), xytext=(i, bottom.max() * 0.42),
                        ha="center", fontsize=8.5, color="#9a6a00",
                        arrowprops=dict(arrowstyle="-|>", color=SIGNAL))
    ax.set_xticks(x, piv.index)
    ax.yaxis.set_major_formatter(_inr)
    ax.grid(axis="y", color=GRID)
    ax.legend(ncol=4, frameon=False, fontsize=8.5, loc="upper right", bbox_to_anchor=(1, 1.02))
    ax.set_ylim(0, bottom.max() * 1.25)
    ax.set_title("gold.daily_merchant_revenue: net revenue (INR) by merchant category, synthetic data")
    return _save(fig, out, "gold_revenue.png")


def masking_sample(silver_c: pd.DataFrame, dim: pd.DataFrame, out: Path) -> Path:
    ids = list(dim["customer_id"].head(3))
    s = silver_c[silver_c["customer_id"].isin(ids) & silver_c["is_current"].astype(bool)].sort_values("customer_id")
    g = dim[dim["customer_id"].isin(ids)].sort_values("customer_id")
    rows = []
    for (_, a), (_, b) in zip(s.iterrows(), g.iterrows()):
        rows.append([a["customer_id"], "silver (restricted)", a["full_name"], a["email"], a["phone"],
                     str(a["date_of_birth"])])
        rows.append(["", "gold.dim_customer", b["full_name"], b["email"][:20] + "…", b["phone"],
                     str(b["date_of_birth_year"])])
    cols = ["customer_id", "layer", "full_name\n(redact)", "email\n(salted SHA-256)", "phone\n(mask_last4)",
            "date_of_birth\n(year_only)"]
    fig, ax = plt.subplots(figsize=(12.5, 2.9))
    ax.axis("off")
    t = ax.table(cellText=rows, colLabels=cols, loc="center", cellLoc="left", colLoc="left",
                 colWidths=[0.09, 0.14, 0.15, 0.31, 0.15, 0.13])
    t.auto_set_font_size(False)
    t.set_fontsize(9)
    t.scale(1, 1.55)
    for (r, c), cell in t.get_celld().items():
        cell.set_edgecolor(GRID)
        if r == 0:
            cell.set_text_props(weight="bold", color=INK)
            cell.set_facecolor("#f3f5f8")
            cell.set_height(cell.get_height() * 1.5)
        elif r % 2 == 0:
            cell.set_facecolor("#fff8e1")
    ax.set_title("Same synthetic customers in silver and in gold: the PII policy applied by the pipeline",
                 loc="left", fontsize=12.5, weight="bold", color=INK)
    return _save(fig, out, "pii_masking.png")


def render_all(cfg: Settings, store: TableStore, out: str | Path | None = None) -> list[Path]:
    out = Path(out) if out else cfg.path("reports")
    out.mkdir(parents=True, exist_ok=True)
    runs = load_runs(store.root)
    return [
        architecture(out),
        layer_flow(runs, out),
        scorecard(store.read("gold", "dq_scorecard"), out),
        quarantine_reasons(store.read("silver", "_quarantine"), out),
        revenue(store.read("gold", "daily_merchant_revenue"), runs, out),
        masking_sample(store.read("silver", "customers"), store.read("gold", "dim_customer"), out),
    ]
