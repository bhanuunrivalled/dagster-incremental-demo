# Local Dagster orders pipeline

A working local pipeline with synthetic data, graphical dataset lineage, persistent run logs, and incremental processing that handles corrections and retries. Dagster 1.13.26 orchestrates three Python assets; SQLite stores the event ledger, current orders, revenue totals, and checkpoints. It has no Foundry, AWS, Spark, or Iceberg dependency.

```text
raw_orders → validated_orders → daily_revenue
```

## Try it in GitHub Codespaces

Open [this repository](https://github.com/bhanuunrivalled/dagster-incremental-demo), choose **Code → Codespaces → Create codespace on main**, and wait for setup to finish. The dev container installs Python 3.12, uv, and the locked dependencies. In its terminal, run:

```bash
uv run orders-demo demo
uv run dagster dev --host 127.0.0.1 --port 3000 -m orders_demo.definitions
```

Open the forwarded **3000 / Dagster lineage** port from the **Ports** tab, then choose **Assets → Global asset lineage** or visit `/asset-groups` on that URL. Keep the port private: the development UI lets its visitor launch jobs. Codespaces usage follows your account's plan; this repository does not create a Codespace automatically.

For a presentation, show the three-asset graph, open **Runs**, and compare the `demo/phase` tags `initial`, `new`, `correction`, and `repeat`. In the correction run, filter logs by `incremental` to see one event change two daily totals. Materialize all three assets again to demonstrate a repeat with zero new changes. The metadata on `daily_revenue` includes the final daily totals.

The pipeline is tested locally and by the included Linux GitHub Actions workflow. The Codespaces configuration is provided, but a hosted Codespace has not been launched for validation.

## Run locally

Use Python 3.12 and [uv](https://docs.astral.sh/uv/). Clone the public demo, then run:

```bash
git clone https://github.com/bhanuunrivalled/dagster-incremental-demo.git
cd dagster-incremental-demo
uv sync --frozen --python 3.12
source .venv/bin/activate
orders-demo demo
pytest -q
orders-demo init
export DAGSTER_HOME="$PWD/.local/dagster"
dagster dev --host 127.0.0.1 --port 3000 -m orders_demo.definitions
```

The lockfile pins dependencies. The virtual environment and all generated state are ignored by Git. `orders-demo init` prepares a project-local Dagster home with telemetry disabled. Keep the server terminal running; Ctrl-C stops it. The pinned version supports `dagster dev`, though it recommends the newer `dg dev` command.

Open [the lineage graph](http://127.0.0.1:3000/asset-groups) and [Runs](http://127.0.0.1:3000/runs). All three assets should be green after the demo. Open a run to see step timing, status, logs, checkpoints, and materialization metadata. Filter the event log by `incremental` for the processing counts. The `daily_revenue` materialization also includes a small table preview. **Materialize all** executes the pipeline again; with unchanged input, it processes zero new events.

After the first demo, reproduce from the beginning without deleting that data by using a new absolute directory:

```bash
export ORDERS_DEMO_ROOT="$PWD/.local/my-second-demo"
orders-demo demo
```

Restart the UI with the same environment variable to display materializations against that store. The full demo refuses to reset an existing populated store. To continue an existing store, use the individual commands below.

## Observe each incremental step

In a fresh `ORDERS_DEMO_ROOT`, run these commands in order:

```bash
orders-demo seed initial
orders-demo run --label initial
orders-demo seed new
orders-demo run --label new
orders-demo seed correction
orders-demo run --label correction
orders-demo run --label repeat
orders-demo show
```

All amounts below are synthetic EUR values; storage uses integer cents.

| Phase | Newly ingested events | Revenue changes applied | Current orders | Total revenue |
| --- | ---: | ---: | ---: | ---: |
| Initial | 3 | 3 | 3 | €250.00 |
| New order | 1 | 1 | 4 | €325.00 |
| Correction | 1 | 1 | 4 | €335.00 |
| Repeat | 0 | 0 | 4 | €335.00 |

The correction changes `ORD-002` from €80 on 12 October to €90 on 13 October. The pipeline subtracts its previous contribution and adds its replacement. Final daily totals are €120 on 12 October and €215 on 13 October. The full demo saves its four real Dagster run IDs, per-asset processing counts, and snapshots in `.local/orders-demo/demo-results.json` (or your chosen root).

## Incremental and failure behavior

Append JSON batch files to the input directory. Each event has a globally unique `event_id`, an `order_id`, a positive integer `version`, an ISO `order_date`, and nonnegative integer `amount_cents`. Corrections are new events with a higher version for the same order. Repeated delivery of identical event IDs is ignored; conflicting payloads for the same ID are rejected. Older order versions are ignored; contradictory equal versions fail validation.

`raw_orders` appends new events and records each input file's hash in one SQLite transaction. Already ingested files cannot be edited or removed: append a correction instead. File discovery reads and hashes previous batches to enforce immutability, but decodes only new batches.

`validated_orders` reads only raw sequence numbers beyond its checkpoint. It updates changed orders and appends their revenue deltas in the same transaction as its checkpoint. `daily_revenue` consumes only new deltas, updates the affected days, and commits its checkpoint with those updates. No output table is cleared or rebuilt on a normal run. Preview metadata reads current output rows; that read does not reprocess the event history.

SQLite transactions use WAL, full synchronous writes, and a write lock. A failure before commit rolls back output and checkpoint together. If a commit succeeds but Dagster does not receive the result, reexecution observes the committed checkpoint and does not apply the changes again. The tests inject failures both before and after commit, including a failed real Dagster run followed by successful reexecution.

Atomicity is per asset, not across the entire pipeline. After a downstream failure, upstream assets can be ahead of revenue until a successful retry catches up. Invalid records stop validation without advancing its checkpoint; quarantine and an operator repair workflow are future work. This demo supports upserts, not deletion events. SQLite and the complete in-memory pending batches suit one local machine and small data; distributed execution, schema migrations, historical backfills, and production recovery remain future work.

## Files and boundaries

- `src/orders_demo/domain.py`: order validation and correction rules, with no Dagster or SQLite imports.
- `src/orders_demo/store.py`: transactional SQLite adapter and incremental checkpoints.
- `src/orders_demo/definitions.py`: real Dagster assets, resource, job, logs, and metadata.
- `src/orders_demo/cli.py`: immutable sample batches, persistent Dagster execution, and inspection commands.
- `sample_data/`: three synthetic input batches.
- `tests/test_pipeline.py`: 21 checks for discovery, the four-phase flow, duplicate and late events, validation failures, correction moves, transaction rollback, lost acknowledgments, overlapping consumers, and real Dagster reexecution.
- `.local/orders-demo/warehouse.sqlite`: persistent pipeline data; `.local/dagster/`: persistent Dagster runs and logs.

Dagster currently executes Python on this machine. In a later AWS design, Dagster can coordinate dependencies and observe runs while a chosen compute service allocates resources: for example, ECS/Fargate for container tasks, or EKS plus a Spark submission service for distributed Spark. That choice is undecided. Moving state to durable shared storage requires preserving the transaction and idempotency contracts rather than putting this SQLite file on S3. S3 and a table format such as Iceberg can be evaluated later. No AWS resources are provisioned here.

The repository contains source, synthetic fixtures, tests, the dependency lockfile, a Codespaces configuration, and a GitHub Actions test workflow. Generated databases, run logs, credentials, and separate REA research files are excluded. This is an independent prototype; it contains no company data or Palantir source code.

Configuration references: [GitHub Codespaces port forwarding](https://docs.github.com/en/codespaces/developing-in-a-codespace/forwarding-ports-in-your-codespace) and [uv in GitHub Actions](https://docs.astral.sh/uv/guides/integration/github/).
