# Data Pipeline Orchestration with Prefect

## Overview

I extended an existing customer orders data pipeline by turning a manually executed workflow into an automated, observable, and scheduled orchestration system using Prefect.

Before this work, the pipeline depended on manually running each step in
the correct order:

1.  Generate source data
2.  Load raw data
3.  Build and test dbt models
4.  Publish curated tables for analytics

The pipeline logic itself was already complete. My work focused on
adding the orchestration layer around it: tracked executions, retries,
scheduling, and structured logging.

The result is a single Prefect flow that manages the full data
lifecycle:

    check source → extract → load raw → dbt build → publish

Each execution is tracked through Prefect with run history, task states,
retries, and logs.

------------------------------------------------------------------------

## Architecture

The completed workflow connects the existing data platform components
into one managed pipeline.

                     ┌─────────────────────────────────────────┐
                     │              Prefect Flow               │
                     │                                         │
    Schedule ───────▶│ check_source → load → dbt → publish     │
                     │                                         │
                     │ retries · logs · run history             │
                     └─────────────────────────────────────────┘
                                          │
                                          ▼
                                  Metabase Dashboard

The platform now includes:

-   Data generation and ingestion
-   Warehouse storage with DuckDB
-   dbt transformations and tests
-   Published serving tables
-   Prefect orchestration
-   Metabase analytics access

------------------------------------------------------------------------

## Repository Structure

  -----------------------------------------------------------------------
  Path                                Purpose
  ----------------------------------- -----------------------------------
  `pipeline/steps.py`                 Existing pipeline functions for
                                      loading, transformation, and
                                      publishing

  `pipeline/flow.py`                  Prefect workflow definition and
                                      orchestration logic

  `pipeline/generate_data.py`         Source data generation process

  `models/`                           dbt models and tests

  `dbt_project.yml`                   dbt project configuration

  `profiles.yml`                      dbt connection configuration

  `docker-compose.yml`                Metabase deployment

  `metabase/`                         Analytics environment configuration
  -----------------------------------------------------------------------

------------------------------------------------------------------------

## Data Publishing Design

The project uses two DuckDB files intentionally.

The main warehouse database:

    data/warehouse.duckdb

is used by dbt for building models and running tests.

The published serving database:

    data/serving/warehouse.duckdb

contains the analytics-ready tables consumed by Metabase.

Separating these databases prevents dashboard access from interfering
with warehouse writes. DuckDB uses a single-writer model, so keeping the
serving layer separate allows scheduled pipeline runs while dashboards
remain connected.

------------------------------------------------------------------------

## Running the Pipeline

Install dependencies:

``` bash
uv sync
```

Run the complete pipeline:

``` bash
uv run python -m pipeline.flow
```

A successful run completes the full process:

-   source data generation
-   raw data loading
-   dbt model creation
-   dbt testing
-   publishing tables to the serving database

------------------------------------------------------------------------

## Prefect Orchestration

I converted the pipeline from a regular Python script into a Prefect
flow.

The flow adds:

-   task tracking
-   execution history
-   state management
-   logging
-   retries
-   scheduling

Start the Prefect server:

``` bash
uv run prefect server start
```

Configure the local Prefect API:

``` bash
uv run prefect config set PREFECT_API_URL=http://127.0.0.1:4200/api
```

Run the flow:

``` bash
uv run python -m pipeline.flow
```

------------------------------------------------------------------------

## Scheduling

The pipeline was configured as a scheduled Prefect deployment.

The scheduled flow:

    nightly-orders

runs automatically according to its configured cron schedule.

Scheduling removes the need for manual execution and allows the platform
to operate continuously.

------------------------------------------------------------------------

## Reliability Improvements

### Retries

The source validation step was configured with retries to handle
temporary upstream failures.

Retries make the workflow more resilient without requiring changes to
the underlying pipeline logic.

### Idempotency

Because scheduled pipelines and retries can execute the same steps more
than once, each operation was reviewed for safe repeated execution.

Using replacement-style database operations allows the workflow to
rebuild outputs consistently instead of creating duplicate records.

------------------------------------------------------------------------

## Logging

The pipeline was updated from simple print statements to Prefect task
logging.

Each task now records meaningful execution information through Prefect
logs, including:

-   task progress
-   retry events
-   execution details
-   failures

These logs are attached directly to workflow runs, making
troubleshooting easier.

------------------------------------------------------------------------

## Metabase Integration

Metabase connects to the published serving database:

    /serving/warehouse.duckdb

The database connection uses read-only access because the serving
database is intended for analytics consumption.

The dashboard layer now receives refreshed data after scheduled pipeline
executions.

------------------------------------------------------------------------

## Validation

The completed orchestration system demonstrates:

-   successful Prefect flow executions
-   tracked pipeline history
-   scheduled deployments
-   retry handling during failures
-   task-level logging
-   refreshed analytics output

The final platform provides a complete production-style data workflow:

    ingest → warehouse → transform → test → publish → analyze

with orchestration managing execution from start to finish.

------------------------------------------------------------------------

## Common Commands

``` bash
# Start Prefect server
uv run prefect server start

# Run pipeline
uv run python -m pipeline.flow

# Simulate upstream failures
SOURCE_FLAKINESS=0.5 uv run python -m pipeline.flow

# Run dbt directly
uv run dbt build

# Start Metabase
docker compose up -d --build
```

------------------------------------------------------------------------

## Troubleshooting

### Prefect UI shows no runs

Confirm the Prefect API URL points to the local server and that the
Prefect server process is running.

### Database locked errors

Ensure only one process is writing to the warehouse database. The
serving copy exists to prevent dashboard connections from locking the
active warehouse.

### Metabase connection issues

Verify:

-   the serving database path is correct
-   the connection is read-only
-   the serving database exists

### Stale dashboard data

Refresh the Metabase database schema after a new pipeline publication.

------------------------------------------------------------------------

## Final Result

This project transformed a manually executed data pipeline into an
automated orchestration system.

The completed platform combines:

-   ingestion
-   warehouse storage
-   transformation
-   testing
-   publishing
-   scheduling
-   observability

The pipeline can now execute reliably with Prefect managing the workflow
lifecycle.
