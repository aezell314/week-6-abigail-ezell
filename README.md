# Week 6 — Pipelines Shouldn't Need You

Everything you've built so far has one dependency left: **you**, typing commands
in the right order. Generate, load, `dbt build`, publish — you are the
orchestrator, and you don't scale, forget nothing, or work at 3am.

This week the whole course becomes **one program** — and then you promote that
program to a **Prefect flow**: tracked runs, retries on the flaky edges, a
schedule, and real logs. Not by rewriting the pipeline — by *decorating* it.

```
              ┌───────────────────────── one flow ──────────────────────────┐
   cron       │ check_source → extract → load raw → dbt build → publish      │
 ┌────────┐   │   (flaky!)              (DuckDB)   (models+tests) (serving DB)│
 │ Prefect│ ─►│                                                             │
 │ server │◄──│         every run tracked · retries · logs                  │
 └────────┘   └─────────────────────────────────────────────────────────────┘
  localhost:4200                                     └─► Metabase (Week 5)
```

The pipeline (`generate → load → dbt build + test`) is **done and provided** —
it runs green out of the box. Your job this week is the ring around it: Prefect.

## What's in this repo

| Path | What it is |
| --- | --- |
| `pipeline/steps.py` | The course, as functions: load, dbt build, publish. **Provided — read it.** |
| `pipeline/flow.py` | **YOUR file.** A plain script on Day 0; a scheduled, retrying, logging flow by Day 3. |
| `pipeline/generate_data.py` | The Week 2 data generator (stands in for the real source). **Provided.** |
| `models/`, `dbt_project.yml`, `profiles.yml` | Your Week 4/5 dbt project — models **and** tests, **provided and passing**. |
| `docker-compose.yml`, `metabase/` | Week 5's Metabase, reading a published copy of your gold tables. |

**Two DuckDB files, on purpose.** `dbt build` writes `data/warehouse.duckdb`
(the live warehouse). The `publish` step copies the gold tables into
`data/serving/warehouse.duckdb` — the copy Metabase reads. Why two? DuckDB is
single-writer: if Metabase held the live warehouse open, the next `dbt build`
couldn't get the lock. Splitting the serving copy out is what lets the pipeline
re-run on a schedule while a dashboard stays connected.

## One-time setup

```bash
uv sync                                   # 1. environment (includes Prefect)
uv run python -m pipeline.flow            # 2. the whole course, one command
```

Step 2 must print `pipeline complete` — source data generated, raw loaded, dbt
models + tests green, gold tables published to `data/serving/`. If that runs,
your baseline is solid and everything below is about Prefect.

Then point Prefect's CLI at your local server (once):

```bash
uv run prefect config set PREFECT_API_URL=http://127.0.0.1:4200/api
```

### See the data — connect Metabase (do this once, up front)

```bash
docker compose up -d --build              # Week 5's Metabase (first build is slow)
```

Open http://localhost:3000, then **Settings → Admin → Databases → Add database**:

| Field | Value |
| --- | --- |
| Database type | **DuckDB** |
| Database file | `/serving/warehouse.duckdb` |
| Read-only | **ON** ← required |

`/serving` is where the container sees `./data/serving`. **Read-only must be ON**
— the file is mounted read-only, so a read-write connection can't open it (that's
the serving pattern, not a bug). Now build a question on `customer_order_summary`
or `clean_orders` — those are the gold tables the pipeline publishes. That's your
baseline: source → warehouse → tests → serving copy → dashboard, all by hand.
The rest of the week makes it run itself.

## The week, day by day

**The rhythm: two terminals.** Terminal 1 runs the Prefect server + UI;
Terminal 2 runs your flow. All week.

```bash
# Terminal 1 — leave it running
uv run prefect server start               # UI at http://localhost:4200

# Terminal 2 — your flow
uv run python -m pipeline.flow
```

### Day 1 — From script to flow

1. Run the pipeline plain (setup step 2) and appreciate the problem: it worked,
   but nothing remembers it worked. No history, no timings, no retry, no
   schedule. `flow.py`'s docstring is your map for fixing that.
2. Follow the **Day 1** items in `flow.py`: `@task` on the five steps,
   `@flow(log_prints=True)` on `run_pipeline`. Run it again.
3. Open http://localhost:4200 → *Runs*. Find your run, click into it: every
   task, its state, its duration, its logs. That page is what "orchestrated"
   means — same pipeline, but now it's *observable*.

### Day 2 — Schedules, retries, and breaking things on purpose

1. **Schedule it.** Follow the Day 2 TODO at the bottom of `flow.py`: swap the
   one-shot call for `run_pipeline.serve(name="nightly-orders", cron="0 6 * * *")`.
   Start it — it registers a *deployment* and waits. In the UI, find
   **Deployments → nightly-orders**, see the next scheduled run, and trigger one
   now with *Quick run*. (For class, try `cron="* * * * *"` — every minute — and
   watch two runs happen without you.)
2. **Break it.** Stop serving. Our upstream source flakes on demand:
   ```bash
   SOURCE_FLAKINESS=1.0 uv run python -m pipeline.flow
   ```
   Watch `check_source` fail and take the whole run down. Find the FAILED run in
   the UI — red, honest, with the traceback attached.
3. **Fix it with configuration, not code.** Give `check_source` (and only
   `check_source`) retries: `@task(retries=3, retry_delay_seconds=5)`. Run with
   `SOURCE_FLAKINESS=0.5` a few times — watch the log say
   `Retry 1/3 will start 5 second(s) from now`, then succeed.
4. **The idempotency question** (a point to think about): retries mean steps *run twice*.
   Walk each step and ask "what happens if this runs again?" — why is
   `CREATE OR REPLACE` doing so much quiet heavy lifting here, and which step
   would be dangerous to retry if it appended instead?

### Day 3 — Logging

1. **Prints become logs.** Follow the Day 3 items in `flow.py`:
   `get_run_logger()` in each task, `logger.info(...)` instead of `print`, and
   pick levels deliberately (is "upstream reachable" info? is a retry a
   warning?). Re-run and find your log lines in the UI, attached to the task run
   they came from — that's the difference between logging and printing.
2. **The finale.** With Metabase already connected (setup, above), serve your
   flow on an every-minute cron for a few minutes. Each run republishes
   `data/serving/warehouse.duckdb`; hit **Sync database schema now** in
   Metabase's database settings (or just re-run your question) and confirm the
   dashboard still returns the expected results. The generator is seeded, so
   the numbers stay the same on purpose; the Prefect run history shows that the
   source → warehouse → tests → serving copy → dashboard path ran again on a
   schedule, with retries and logs, and you didn't touch a thing.

## Working commands

```bash
uv run prefect server start                      # terminal 1: server + UI (:4200)
uv run python -m pipeline.flow                   # run once (or serve, per Day 2)
SOURCE_FLAKINESS=0.5 uv run python -m pipeline.flow   # simulate a bad upstream
uv run dbt build                                 # dbt alone still works (models + tests)
docker compose up -d --build                     # Metabase, reading data/serving/
```

## How you'll know you're done

The UI shows: at least one FAILED run (Day 2's deliberate break), a green run
where `check_source` logged a retry, a deployment named `nightly-orders` with a
cron schedule and completed scheduled runs, and task-level log lines with levels
(not prints). Metabase shows fresh data after a scheduled run + a schema sync.

That's the last brick. **You now have every piece of a production data platform:
ingest, warehouse, transform, test, serve, orchestrate.** Project 1 is "make it
yours."

## Troubleshooting

- **UI shows no runs** — your flow talked to a throwaway local API instead of
  the server. Run the `prefect config set PREFECT_API_URL=...` line from setup,
  and make sure Terminal 1 is still running.
- **`.serve()` seems to hang** — it's not hanging, it's *serving*. That process
  stays alive waiting for the cron; Ctrl+C stops it (and the schedule pauses
  when nothing is serving the deployment).
- **`database is locked` on `dbt build`** — something else has
  `warehouse.duckdb` open (a stray REPL, a second flow run). One writer at a
  time; kill the other process. Note this is *not* Metabase — Metabase reads the
  separate serving copy, so it can't lock the warehouse.
- **Metabase won't connect / "Could not set lock"** — make sure **Read-only is
  ON** and the path is exactly `/serving/warehouse.duckdb`. The file is mounted
  read-only, so a read-write connection can't open it.
- **Metabase shows stale numbers after a run** — it's caching its connection to
  the previously published copy. Hit **Sync database schema now** in the database
  settings (or restart the container) to pick up the latest serving copy.
- **Port 4200 or 3000 already in use** — an old `prefect server start` or Week
  5's Metabase is still up; stop it (`docker compose down` for Metabase).
- **Windows** — if VS Code doesn't pick up the env, Command Palette → *Python:
  Select Interpreter* → the one under `.venv`.
