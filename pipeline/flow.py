"""YOUR file. The whole course, as one program — soon, one Prefect flow.

Run it:  uv run python -m pipeline.flow

Day 0 (now): it's a plain script. It works. It also has no memory, no retries,
no schedule, and if it dies at 3am nothing tells you. This week you fix that
WITHOUT rewriting the pipeline — every change below is a decorator or an
argument. That's the pitch of an orchestrator.

Day 1 — promote it to a flow:
  * import from prefect:          from prefect import flow, task
  * put @task on each step function below
  * put @flow(log_prints=True) on run_pipeline
  * run it again, then look at the run in the UI (README has the two terminals)

Day 2 — make it survive the real world:
  * schedule it: swap the run_pipeline() call at the bottom for
        run_pipeline.serve(name="nightly-orders", cron="0 6 * * *")
  * break it: run with SOURCE_FLAKINESS=0.7 and watch check_source fail the run
  * fix it with configuration, not code: @task(retries=3, retry_delay_seconds=5)
    on check_source only — the flaky edge gets retries, the deterministic
    middle doesn't (worth asking yourself why)

Day 3 — say what happened, properly:
  * from prefect.logging import get_run_logger
  * inside each task/flow: logger = get_run_logger(), then swap every print()
    for logger.info(...) (or logger.warning/error — choose levels on purpose)
"""

from pipeline import steps


def check_source() -> None:
    """The step that talks to the outside world — the one that WILL flake."""
    steps.check_source_freshness()
    print("upstream source: reachable")


def extract() -> None:
    """Stand-in for pulling from the real upstream (Week 2 did this against an API)."""
    steps.generate_source_data()
    print("source data extracted")


def load() -> dict[str, int]:
    counts = steps.load_raw()
    print(f"loaded raw: {counts}")
    return counts


def transform() -> None:
    steps.dbt_build()
    print("dbt build: models + tests green")


def publish() -> int:
    """Copy the gold tables to the serving DuckDB Metabase reads (Week 5's serving repo)."""
    count = steps.publish_serving()
    print(f"serving layer published: {count} gold tables -> data/serving/warehouse.duckdb")
    return count


def run_pipeline() -> None:
    """Source to dashboard, in order. Each call below becomes a tracked task run."""
    check_source()
    extract()
    load()
    transform()
    publish()
    print("pipeline complete — Metabase is reading the freshly published serving copy")


if __name__ == "__main__":
    run_pipeline()
    # TODO (Day 2): replace the line above with a schedule —
    #   run_pipeline.serve(name="nightly-orders", cron="0 6 * * *")
    # (.serve() keeps running and fires the flow on the cron; Ctrl+C stops it.
    #  While it's serving, you can also trigger an immediate run from the UI.)
