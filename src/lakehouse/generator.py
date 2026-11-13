"""Synthetic landing-zone drops for a digital-payments wallet.

Every run with the same seed writes the same files. The data is synthetic (names come from
small word lists, emails use example.com) and contains the defects a real pipeline meets:

* exact duplicate rows and repeated deliveries of the same transaction
* null keys, malformed emails, negative amounts, unsupported currencies, malformed JSON lines
* transactions that point at customers that do not exist (orphans)
* customer attribute changes over time (feeds the SCD2 history in silver)
* refunds arriving days later as a newer version of the same ``txn_id``
* day 3: the merchants file arrives with ``category`` renamed to ``merchant_category`` (schema drift)
* day 4: an upstream bug sends ~9% of currencies in lower case, enough to trip the quality gate
* day 5: the transactions feed adds a ``device_os`` field nobody asked for (rescued, not lost)
"""
from __future__ import annotations

import csv
import json
import random
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path

from .config import Settings

FIRST = ["Aarav", "Diya", "Kabir", "Meera", "Rohan", "Isha", "Arjun", "Sana", "Vihaan", "Anaya", "Kiran",
         "Neha", "Rahul", "Priya", "Aman", "Simran", "Dev", "Tara", "Nikhil", "Riya", "Harpreet", "Gurleen"]
LAST = ["Sharma", "Verma", "Gill", "Sandhu", "Iyer", "Nair", "Khan", "Das", "Mehta", "Kapoor", "Reddy",
        "Bose", "Joshi", "Malhotra", "Chauhan", "Grewal", "Bhatia", "Saxena"]
CITIES = ["Mohali", "Chandigarh", "Ludhiana", "Patiala", "Jaipur", "Delhi", "Pune", "Bengaluru", "Mumbai"]
CATEGORIES = ["grocery", "electronics", "fashion", "travel", "food_delivery", "pharmacy", "utilities", "education"]
MERCHANT_WORDS = ["Bazaar", "Mart", "Hub", "Express", "Corner", "Point", "Store", "Depot"]
CHANNELS = ["app", "web", "pos"]

SCHEMA_DRIFT_DAY = 3      # 1-based day index
GATE_TRIP_DAY = 4
NEW_FIELD_DAY = 5


@dataclass
class Truth:
    """What the generator knows to be true; used by tests to check the pipeline end to end."""
    customers: dict[str, dict]
    merchants: dict[str, dict]
    files: list[Path]


def _ts(d: date, rng: random.Random) -> datetime:
    return datetime.combine(d, time(0), tzinfo=timezone.utc) + timedelta(seconds=rng.randrange(86_400))


def _iso(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")


def _customer(i: int, d: date, rng: random.Random) -> dict:
    first, last = rng.choice(FIRST), rng.choice(LAST)
    dob = date(1962, 1, 1) + timedelta(days=rng.randrange(365 * 43))
    return {
        "customer_id": f"C{i:05d}",
        "full_name": f"{first} {last}",
        "email": f"{first.lower()}.{last.lower()}{i}@example.com",
        "phone": f"+91 9{rng.randrange(10**8, 10**9):09d}",
        "date_of_birth": dob.isoformat(),
        "city": rng.choice(CITIES),
        "country": "IN",
        "kyc_tier": rng.choices(["basic", "standard", "premium"], [5, 3, 1])[0],
        "signup_date": d.isoformat(),
        "updated_at": _iso(_ts(d, rng)),
    }


def _merchant(i: int, d: date, rng: random.Random) -> dict:
    return {
        "merchant_id": f"M{i:04d}",
        "merchant_name": f"{rng.choice(LAST)} {rng.choice(MERCHANT_WORDS)}",
        "category": rng.choice(CATEGORIES),
        "city": rng.choice(CITIES),
        "updated_at": _iso(_ts(d, rng)),
    }


def _amount(rng: random.Random, currency: str) -> str:
    inr = round(min(rng.lognormvariate(6.6, 1.0), 200_000), 2)
    if currency != "INR":
        inr = round(inr / (84 if currency == "USD" else 90), 2)
    return f"{max(inr, 1.0):.2f}"


def _write_csv(path: Path, rows: list[dict], header: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=header, extrasaction="ignore")
        w.writeheader()
        w.writerows(rows)


def generate(cfg: Settings) -> Truth:
    g = cfg.generator
    rng = random.Random(g.seed)
    landing = cfg.path("landing")
    customers: dict[str, dict] = {}
    merchants: dict[str, dict] = {}
    onboarded: dict[str, str] = {}
    captured: list[dict] = []        # transactions that can later be refunded
    files: list[Path] = []
    next_c, next_m, next_t = 1, 1, 1
    first_day_customers = int(g.customers * 0.85)
    new_customers_per_day = max(1, (g.customers - first_day_customers) // max(1, g.days - 1))
    first_day_merchants = g.merchants - 2 * (g.days - 1)

    for day_idx in range(1, g.days + 1):
        d = g.start_date + timedelta(days=day_idx - 1)
        ds = d.isoformat()

        # ---- customers: snapshot on day 1, then new sign-ups + attribute changes (CDC-style extract)
        rows: list[dict] = []
        n_new = first_day_customers if day_idx == 1 else new_customers_per_day
        for _ in range(n_new):
            c = _customer(next_c, d, rng)
            next_c += 1
            customers[c["customer_id"]] = c
            rows.append(c)
        if day_idx > 1:
            for cid in rng.sample(sorted(customers), 14):
                c = dict(customers[cid])
                if rng.random() < 0.5:
                    c["city"] = rng.choice([x for x in CITIES if x != c["city"]])
                else:
                    c["kyc_tier"] = {"basic": "standard", "standard": "premium", "premium": "standard"}[c["kyc_tier"]]
                c["updated_at"] = _iso(_ts(d, rng))
                customers[cid] = c
                rows.append(c)
            # the same customer changed twice in one extract: only the later version may win
            cid = rng.choice(sorted(customers))
            older = dict(customers[cid], city=rng.choice(CITIES), updated_at=f"{ds}T01:00:00Z")
            newer = dict(customers[cid], kyc_tier="premium", updated_at=f"{ds}T23:00:00Z")
            customers[cid] = newer
            rows += [newer, older]
        bad = []
        n_bad = 2 if day_idx == 1 else 1
        for _ in range(n_bad):
            bad.append(dict(_customer(90_000 + next_c, d, rng), customer_id=""))
            next_c += 1
        for _ in range(n_bad + 1):
            c = _customer(next_c, d, rng)
            next_c += 1
            c["email"] = c["email"].replace("@", " at ")
            bad.append(c)
        rows += bad + rng.sample(rows, 4)           # 4 exact duplicates
        rng.shuffle(rows)
        path = landing / "customers" / ds / f"customers_{ds}.csv"
        _write_csv(path, rows, list(cfg.entities["customers"].schema))
        files.append(path)

        # ---- merchants: snapshot on day 1, then two onboardings a day + a category change
        rows = []
        if day_idx == 1:
            todays = [_merchant(next_m + i, d, rng) for i in range(first_day_merchants)]
            next_m += first_day_merchants
        else:
            todays = [_merchant(next_m + i, d, rng) for i in range(2)]
            next_m += 2
            mid = rng.choice(sorted(merchants))
            todays.append(dict(merchants[mid], category=rng.choice(CATEGORIES), updated_at=_iso(_ts(d, rng))))
        if day_idx == SCHEMA_DRIFT_DAY + 1:
            # upstream re-sends the rows that were rejected the day before, in the right shape
            todays += [m for m in merchants.values() if m.get("_resend")]
        for m in todays:
            merchants[m["merchant_id"]] = {k: v for k, v in m.items() if k != "_resend"}
            onboarded.setdefault(m["merchant_id"], ds)
        rows = [{k: v for k, v in m.items() if k != "_resend"} for m in todays]
        header = list(cfg.entities["merchants"].schema)
        if day_idx == SCHEMA_DRIFT_DAY:
            header = ["merchant_category" if h == "category" else h for h in header]
            rows = [{("merchant_category" if k == "category" else k): v for k, v in r.items()} for r in rows]
            for m in todays:
                merchants[m["merchant_id"]]["_resend"] = True
        path = landing / "merchants" / ds / f"merchants_{ds}.csv"
        _write_csv(path, rows, header)
        files.append(path)

        # ---- transactions (JSON lines)
        lines: list[str] = []
        cust_ids = sorted(customers)
        merch_ids = sorted(m for m in merchants if onboarded[m] < ds or day_idx == 1)
        merch_new = sorted(set(merchants) - set(merch_ids))
        new_field = day_idx == NEW_FIELD_DAY
        for _ in range(g.transactions_per_day):
            currency = rng.choices(["INR", "USD", "EUR"], [85, 10, 5])[0]
            ts = _ts(d, rng)
            t = {
                "txn_id": f"T{next_t:07d}",
                "customer_id": rng.choice(cust_ids),
                "merchant_id": rng.choice(merch_new if merch_new and rng.random() < 0.02 else merch_ids),
                "amount": _amount(rng, currency),
                "currency": currency,
                "status": rng.choices(["captured", "failed"], [94, 6])[0],
                "channel": rng.choice(CHANNELS),
                "txn_ts": _iso(ts),
                "updated_at": _iso(ts),
            }
            next_t += 1
            if new_field:
                t["device_os"] = rng.choice(["android", "ios", "web"])
            if day_idx == GATE_TRIP_DAY and rng.random() < 0.09:
                t["currency"] = t["currency"].lower()
            if t["status"] == "captured":
                captured.append(t)
            lines.append(json.dumps(t))
        # refunds for earlier days: a newer version of an existing txn_id
        earlier = [t for t in captured if t["txn_ts"][:10] < ds]
        for t in rng.sample(earlier, min(len(earlier), 40)):
            r = dict(t, status="refunded", updated_at=_iso(_ts(d, rng)))
            if t.get("currency", "").islower():
                r["currency"] = t["currency"].upper()
            lines.append(json.dumps(r))
            captured.remove(t)
        # defects
        def tx(**over):
            nonlocal next_t
            base = json.loads(rng.choice(lines))
            base["txn_id"] = f"T{next_t:07d}"
            base.update(over)
            next_t += 1
            return json.dumps(base)
        lines += [tx(txn_id=None) for _ in range(4)]
        lines += [tx(amount=rng.choice(["-120.00", "0.00", "-5.50"])) for _ in range(6)]
        lines += [tx(currency="GBP") for _ in range(5)]
        lines += [tx(customer_id=f"C9{rng.randrange(10**4):04d}") for _ in range(4)]
        lines += rng.sample(lines, 25)                                  # duplicate deliveries
        lines += ['{"txn_id": "T-broken", "amount": 12.5, ', "not json at all"]
        rng.shuffle(lines)
        path = landing / "transactions" / ds / f"transactions_{ds}.jsonl"
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("\n".join(lines) + "\n")
        files.append(path)

    for m in merchants.values():
        m.pop("_resend", None)
    return Truth(customers=customers, merchants=merchants, files=files)


def batch_dates(cfg: Settings) -> list[str]:
    return [(cfg.generator.start_date + timedelta(days=i)).isoformat() for i in range(cfg.generator.days)]
