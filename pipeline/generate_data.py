"""Generate synthetic source data. Produces two files under ``data/source/``:

  - orders.csv      ~30,000 logical orders, but MORE rows than that because some
                    are re-ingested. Each row carries an ``updated_at`` so you can
                    tell which copy is newest. The mess, on purpose:
                      * DUPLICATES — ~6% of orders appear 2-3 times with a later
                        ``updated_at`` (and sometimes a changed ``status``).
                        -> Day 1: ROW_NUMBER() dedup, keep the latest.
                      * NULL JOIN KEYS — ~2% have a blank ``customer_id``.
                        -> Day 1/3: NULL handling, and the INNER-JOIN "NULL trap".
                      * MONEY AS TEXT — ``price`` is a string like "$1,209.50",
                        " 42.99 ", or "" (blank). -> Day 3: stripping + casting.
                      * THREE DATE FORMATS — ``order_date`` is "05-Jan-2024" OR
                        "2024-01-05" OR "01/05/2024", chosen at random.
                        -> Day 3: COALESCE over several TRY_STRPTIME attempts.
                      * MIXED STATUS — casing + stray spaces, plus some blank.
                        -> Day 1: normalize, COALESCE blanks to 'unknown'.
                      * blank ``quantity`` on a sprinkling of rows. -> drop.

  - customers.json  ~4,000 customers (semi-structured), also re-ingested:
                      * DUPLICATES — ~5% appear twice with a higher
                        ``record_version``. -> dedup before joining or your order
                        counts double (a join fan-out trap).
                      * nested ``address`` with some NULL ``zip`` / ``city``.
                      * a ``tags`` list -> the Polars explode/aggregate transform.

Everything is seeded, so re-running gives identical files. Standard library only.

Run it with:  uv run python -m pipeline.generate_data
"""

from __future__ import annotations

import csv
import json
import random
from datetime import date, datetime, timedelta
from pathlib import Path

N_ORDERS = 30_000
N_CUSTOMERS = 4_000
SEED = 42

OUT_DIR = Path("data/source")

FIRST_NAMES = [
    "Ava",
    "Liam",
    "Maya",
    "Noah",
    "Zoe",
    "Ethan",
    "Iris",
    "Owen",
    "Nora",
    "Leo",
    "Ruby",
    "Eli",
    "June",
    "Max",
    "Cora",
    "Finn",
    "Hazel",
    "Jude",
]
LAST_NAMES = [
    "Reyes",
    "Tran",
    "Patel",
    "Nguyen",
    "Walsh",
    "Kim",
    "Okafor",
    "Diaz",
    "Cohen",
    "Ross",
    "Mbeki",
    "Sato",
    "Flores",
    "Park",
    "Haddad",
    "Novak",
]
CITIES = [
    ("Nashville", "TN"),
    ("Austin", "TX"),
    ("Denver", "CO"),
    ("Portland", "OR"),
    ("Atlanta", "GA"),
    ("Boston", "MA"),
    ("Chicago", "IL"),
    ("Seattle", "WA"),
]
PRODUCTS = [
    ("WIDGET-01", 12.50),
    ("WIDGET-02", 18.00),
    ("GADGET-07", 42.99),
    ("GADGET-12", 7.25),
    ("SPROCKET-3", 99.00),
    ("SPROCKET-9", 145.50),
    ("CABLE-USB", 9.99),
    ("CABLE-HDMI", 14.49),
    ("MOUNT-PRO", 31.00),
    ("MOUNT-LITE", 22.75),
]
# Mixed casing + whitespace on purpose, so students have something to normalize.
STATUSES = ["completed", "Completed", "SHIPPED", "shipped", "pending", " Pending ", "cancelled"]
# A later re-ingestion often advances the status; this is the "newer" pool.
STATUSES_ADVANCED = ["completed", "Completed", "shipped", "SHIPPED"]
TAG_POOL = ["new", "vip", "wholesale", "newsletter", "churn-risk", "referral"]


def format_money(value: float, rng: random.Random) -> str:
    """Render a price the way the messy upstream does: as TEXT.

    Sometimes with a "$", sometimes with a thousands comma, sometimes padded with
    spaces. The point is that it does NOT arrive as a clean number.
    """
    style = rng.random()
    if style < 0.55:
        return f"${value:,.2f}"  # "$1,209.50" or "$42.99"
    if style < 0.80:
        return f"{value:.2f}"  # "42.99"
    if style < 0.92:
        return f"  {value:.2f} "  # padded: "  42.99 "
    return f"${value:.2f}"  # "$42.99" (no comma even when large)


def format_date(d: date, rng: random.Random) -> str:
    """One of three date formats, chosen at random — the Day-3 date headache."""
    style = rng.random()
    if style < 0.50:
        return d.strftime("%d-%b-%Y")  # "05-Jan-2024"  (DuckDB won't auto-detect)
    if style < 0.80:
        return d.isoformat()  # "2024-01-05"
    return d.strftime("%m/%d/%Y")  # "01/05/2024"


def generate_customers(rng: random.Random) -> list[dict]:
    """Distinct customers, then sprinkle in duplicate re-ingestions."""
    customers: list[dict] = []
    for cid in range(1, N_CUSTOMERS + 1):
        city, state = rng.choice(CITIES)
        # ~8% of addresses are missing a zip; ~3% missing the city entirely.
        zip_code: object = f"{rng.randint(10000, 99999)}"
        if rng.random() < 0.08:
            zip_code = None
        city_value: object = city
        if rng.random() < 0.03:
            city_value = None
        customers.append(
            {
                "customer_id": cid,
                "name": f"{rng.choice(FIRST_NAMES)} {rng.choice(LAST_NAMES)}",
                "email": f"user{cid}@example.com",
                "signup_date": str(date(2023, 1, 1) + timedelta(days=rng.randint(0, 700))),
                "record_version": 1,
                "address": {"city": city_value, "state": state, "zip": zip_code},
                "tags": rng.sample(TAG_POOL, k=rng.randint(0, 3)),
            }
        )

    # ~5% get a second, newer record (record_version 2) with tweaked tags — the
    # duplicate-customer trap. Keep the highest record_version when you dedup.
    duplicates: list[dict] = []
    for cust in customers:
        if rng.random() < 0.05:
            newer = json_roundtrip(cust)
            newer["record_version"] = 2
            newer["tags"] = rng.sample(TAG_POOL, k=rng.randint(0, 3))
            duplicates.append(newer)
    combined = customers + duplicates
    rng.shuffle(combined)
    return combined


def json_roundtrip(obj: dict) -> dict:
    """Cheap deep copy via json (stdlib only) so we don't mutate the original."""
    return json.loads(json.dumps(obj))


def generate_orders(rng: random.Random) -> list[dict]:
    """One logical row per order, then re-ingest some as duplicates."""
    start = date(2024, 1, 1)
    base_ts = datetime(2024, 1, 1, 0, 0, 0)
    orders: list[dict] = []
    for oid in range(1, N_ORDERS + 1):
        sku, base_price = rng.choice(PRODUCTS)
        order_day = start + timedelta(days=rng.randint(0, 364))

        # ~2% of orders have no customer_id (a NULL join key).
        customer_id: object = rng.randint(1, N_CUSTOMERS)
        if rng.random() < 0.02:
            customer_id = ""

        quantity: object = rng.randint(1, 6)
        if rng.random() < 0.015:
            quantity = ""  # blank quantity -> unusable, drop it

        price_value = round(base_price * rng.uniform(0.9, 1.1), 2)
        price: object = format_money(price_value, rng)
        if rng.random() < 0.015:
            price = ""  # blank price -> unusable, drop it

        # An ingestion timestamp, so a later copy of the same order wins.
        ingested = base_ts + timedelta(days=(order_day - start).days, minutes=rng.randint(0, 600))

        orders.append(
            {
                "order_id": oid,
                "customer_id": customer_id,
                "sku": sku,
                "quantity": quantity,
                "price": price,
                "status": rng.choice(STATUSES),
                "order_date": format_date(order_day, rng),
                "updated_at": ingested.isoformat(sep=" "),
            }
        )

    # ~6% of orders get re-ingested 1-2 more times with a LATER updated_at and an
    # often-advanced status. Same order_id, newer row -> ROW_NUMBER() dedup.
    duplicates: list[dict] = []
    for row in orders:
        if rng.random() < 0.06:
            for _ in range(rng.randint(1, 2)):
                newer = dict(row)
                later = datetime.fromisoformat(row["updated_at"]) + timedelta(
                    hours=rng.randint(1, 72)
                )
                newer["updated_at"] = later.isoformat(sep=" ")
                newer["status"] = rng.choice(STATUSES_ADVANCED)
                duplicates.append(newer)
    combined = orders + duplicates
    rng.shuffle(combined)
    return combined


def main() -> None:
    rng = random.Random(SEED)
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    customers = generate_customers(rng)
    customers_path = OUT_DIR / "customers.json"
    with customers_path.open("w") as f:
        json.dump(customers, f, indent=2)

    orders = generate_orders(rng)
    orders_path = OUT_DIR / "orders.csv"
    fields = [
        "order_id",
        "customer_id",
        "sku",
        "quantity",
        "price",
        "status",
        "order_date",
        "updated_at",
    ]
    with orders_path.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        writer.writerows(orders)

    distinct_orders = len({row["order_id"] for row in orders})
    distinct_customers = len({c["customer_id"] for c in customers})
    print(
        f"wrote {len(orders):,} order rows ({distinct_orders:,} distinct order_ids) "
        f"-> {orders_path}"
    )
    print(
        f"wrote {len(customers):,} customer rows ({distinct_customers:,} distinct customer_ids) "
        f"-> {customers_path}"
    )
    print("note: row counts > distinct counts on purpose — that's the duplicate mess to dedup.")


if __name__ == "__main__":
    main()
