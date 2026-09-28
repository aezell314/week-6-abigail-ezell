"""The pipeline's steps, as importable functions. **Provided — read, don't rewrite.**

Nothing in here is new. This is the work you've been doing all course, gathered
into one place so a single program can run it end to end:

    check_source_freshness  -> the step that talks to the outside world (Week 2's API lesson)
    generate_source_data    -> stands in for "extract from the real source"
    load_raw                -> files into DuckDB, as-is (Weeks 1-4)
    dbt_build               -> your models + tests (Week 4)
    publish_serving         -> the serving copy Metabase reads (Week 5)

What IS new this week is everything AROUND these functions: tracking, retries,
schedules, logs. That's flow.py — your file.
"""

from __future__ import annotations

import os
import random
import subprocess
import sys
from contextlib import chdir
from pathlib import Path

import duckdb

from pipeline.generate_data import main as _generate

REPO_ROOT = Path(__file__).resolve().parent.parent
WAREHOUSE = REPO_ROOT / "data" / "warehouse.duckdb"
SOURCE_DIR = REPO_ROOT / "data" / "source"
SERVING_DIR = REPO_ROOT / "data" / "serving"
SERVING_DB = SERVING_DIR / "warehouse.duckdb"

# The serving-layer contract — the gold tables Metabase reads. Same deliberate
# subset as Week 5's serving repo.
GOLD_TABLES = ["clean_orders", "customer_order_summary"]


def check_source_freshness() -> None:
    """Ping the upstream source before doing any work.

    Our 'upstream' is simulated, but its failure mode is painfully real:
    networks flake, APIs rate-limit, vendors deploy on Fridays. Set the
    SOURCE_FLAKINESS env var (0.0-1.0) to control how often this blows up —
    Day 2 turns it up on purpose.
    """
    flakiness = float(os.environ.get("SOURCE_FLAKINESS", "0"))
    if random.random() < flakiness:
        raise ConnectionError("upstream source did not respond (simulated flake — retry me)")


def generate_source_data() -> None:
    """Produce data/source/ — the stand-in for extracting from the real source."""
    with chdir(REPO_ROOT):
        _generate()


def load_raw() -> dict[str, int]:
    """Load the source files into DuckDB as-is. Returns {table: row_count}."""
    WAREHOUSE.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(WAREHOUSE))
    try:
        con.execute("CREATE SCHEMA IF NOT EXISTS raw")
        con.execute(
            "CREATE OR REPLACE TABLE raw.orders AS SELECT * FROM read_csv_auto(?)",
            [str(SOURCE_DIR / "orders.csv")],
        )
        con.execute(
            "CREATE OR REPLACE TABLE raw.customers AS SELECT * FROM read_json_auto(?)",
            [str(SOURCE_DIR / "customers.json")],
        )
        return {
            "raw.orders": con.execute("SELECT count(*) FROM raw.orders").fetchone()[0],
            "raw.customers": con.execute("SELECT count(*) FROM raw.customers").fetchone()[0],
        }
    finally:
        con.close()


def dbt_build() -> None:
    """Run `dbt build` — models AND tests — as a subprocess. Raises if anything fails.

    A subprocess, not an import, for two reasons: it's how orchestrators call
    dbt in the wild, and dbt's in-process runner would keep the warehouse file
    attached and block the publish step from opening it.
    """
    dbt = Path(sys.executable).parent / "dbt"
    result = subprocess.run([str(dbt), "build"], cwd=REPO_ROOT)
    if result.returncode != 0:
        raise RuntimeError("dbt build failed — a model or test is broken (see output above)")


def publish_serving() -> int:
    """Publish the gold tables to the serving DuckDB that Metabase reads.

    dbt writes the live warehouse; Metabase reads THIS copy — two separate
    files on purpose. DuckDB is single-writer: a live dashboard holding
    warehouse.duckdb open (even read-only) would block the next dbt rebuild.
    Splitting the serving copy out is what lets the pipeline re-run on a
    schedule while Metabase stays connected. (Week 5 used DuckLake for this
    decoupling; a plain published copy is the simpler version of the same idea.)

    We build the serving file under a temp name and atomically swap it into
    place, so Metabase never opens a half-written file. Returns the number of
    gold tables published.
    """
    SERVING_DIR.mkdir(parents=True, exist_ok=True)
    tmp = SERVING_DIR / "warehouse.duckdb.tmp"
    tmp.unlink(missing_ok=True)
    # Attach the warehouse (read-only) and the serving copy (write target) onto
    # an in-memory root. The attach alias MUST be `warehouse`: staging models are
    # views, and dbt compiled their bodies with the catalog qualified by name
    # ("warehouse"."main"."orders_deduped"), so the view only resolves when the
    # file is attached under that exact name. We read from warehouse, write into
    # serving.
    con = duckdb.connect()
    try:
        con.execute(f"ATTACH '{WAREHOUSE}' AS warehouse (READ_ONLY)")
        con.execute(f"ATTACH '{tmp}' AS serving")
        for table in GOLD_TABLES:
            con.execute(
                f"CREATE OR REPLACE TABLE serving.main.{table} "
                f"AS SELECT * FROM warehouse.main.{table}"
            )
    finally:
        con.close()
    # Atomic swap: the old serving file is replaced in one step, so a query
    # arriving mid-publish sees either the whole old file or the whole new one.
    os.replace(tmp, SERVING_DB)
    return len(GOLD_TABLES)
