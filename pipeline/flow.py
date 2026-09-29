"""How to run this file:  uv run python -m pipeline.flow
"""

from prefect import flow, task
from prefect.logging import get_run_logger

from pipeline import steps


@task(retries=3, retry_delay_seconds=5)
def check_source() -> None:
  """The step that talks to the outside world — the one that will flake."""
  logger = get_run_logger()

  try:
    steps.check_source_freshness()
    logger.info("upstream source: reachable")
  except Exception as exc:
    logger.warning(
        f"Flake detected in check_source_freshness. Retrying... Error: {exc}"
    )
    raise

@task
def extract() -> None:
    """Stand-in for pulling from the real upstream."""
    logger = get_run_logger()
    steps.generate_source_data()
    logger.info("source data extracted")

@task
def load() -> dict[str, int]:
    """Load raw data into DuckDB."""
    logger = get_run_logger()
    counts = steps.load_raw()
    logger.info(f"loaded raw: {counts}")
    return counts

@task
def transform() -> None:
    """Run data transformations."""
    logger = get_run_logger()
    steps.dbt_build()
    logger.info("dbt build: models + tests green")

@task
def publish() -> int:
    """Copy the gold tables to the serving DuckDB Metabase reads."""
    logger = get_run_logger()
    count = steps.publish_serving()
    logger.info(f"serving layer published: {count} gold tables -> data/serving/warehouse.duckdb")
    return count

@flow(log_prints=True)
def run_pipeline() -> None:
    logger = get_run_logger()
    check_source()
    extract()
    load()
    transform()
    publish()
    logger.info("pipeline complete — Metabase is reading the freshly published serving copy")


if __name__ == "__main__":
    run_pipeline.serve(name="nightly-orders", cron="0 6 * * *")
