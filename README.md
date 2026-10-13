# Foundry-style pipelines on local Spark

Keep the familiar `from transforms.api import ...` authoring model. This prototype implements a **limited, independent compatibility layer** for `@transform`, `@transform_df`, `@incremental`, `Input`, `Output`, and `Pipeline`.

Pipeline authors write Python/Spark transformations. The platform discovers those transformations, generates dataset lineage, and runs them through Dagster. The transform files contain **no Dagster imports or asset decorators**.

This is an API compatibility experiment, not Palantir's library or a complete Foundry replacement. Compatibility with a company's existing repositories has not been established. The supported subset and explicit limitations are in [COMPATIBILITY.md](docs/COMPATIBILITY.md).

## The code your team writes

Start with these actual pipeline files:

- [Incremental deduplication](src/foundry_example/datasets/deduplicate.py): familiar decorators and input/output objects; the compute body follows the public Palantir sample unchanged.
- [A DataFrame transform](src/foundry_example/datasets/counts.py): `@transform_df` and ordinary Spark aggregation.
- [Pipeline registration](src/foundry_example/pipeline.py): `Pipeline.discover_transforms(...)` finds the transform modules automatically.

The generated lineage is:

```text
/demo/students → /demo/unique_students → /demo/hair_counts
```

The adapter and Dagster configuration live separately under `src/foundry_compat/`. Existing dataset paths or IDs can be used as logical identifiers, but source ingestion/storage mappings still need operator configuration. Matching import names alone does not guarantee compatibility with every Foundry feature.

## Try it in GitHub Codespaces

Choose **Code → Codespaces → Create codespace on main**. The dev container installs Python 3.12, Java 17, PySpark 3.5.7, and locked dependencies. In an existing Codespace, rebuild the container after pulling this update.

```bash
uv run foundry-demo demo
uv run foundry-demo ui
```

Open port **3000** from the **Ports** tab, then open **Assets → Global asset lineage** (or `/asset-groups`). Keep the port private because the development UI can launch jobs. The hosted Codespace configuration has not been exercised; no Codespace is automatically created by these files.

## Run on your own computer

Install Python 3.12, a Java 17 JDK, and [uv](https://docs.astral.sh/uv/). `JAVA_HOME` must point to that JDK. On macOS with Java 17 installed, `export JAVA_HOME="$(/usr/libexec/java_home -v 17)"` selects it.

```bash
git clone https://github.com/bhanuunrivalled/dagster-incremental-demo.git
cd dagster-incremental-demo
uv sync --frozen
uv run foundry-demo demo
uv run foundry-demo ui
```

The UI binds to `127.0.0.1:3000`. If the earlier orders demo is still using that port, use `uv run foundry-demo ui --port 3001`. Stop the server with Ctrl-C. Java is needed for Spark execution, not merely for importing pipeline definitions.

## What to demonstrate

The command creates four real Dagster runs with `demo/phase` tags:

| Phase | Incoming student IDs | Unique output IDs | Execution |
| --- | --- | --- | --- |
| Initial | A, A, B | A, B | Full build |
| New batch | B, C | A, B, C | Incremental; writes only C |
| Repeat | No new batch | A, B, C | Incremental; writes zero rows |
| Source replaced | D replaces the source | D | Full rebuild; old output removed |

Open **Runs**, select a phase, and inspect the `Foundry-style transform` logs. The materializations include incremental status, write mode, row count written, and committed output revision. The final source has been replaced with D, so both downstream datasets reflect that replacement. Materialize the downstream assets again to repeat the last state.

The source asset is external: the demo command ingests its synthetic batches. It appears in lineage but has no transform to materialize. Each transform commits independently; a failed downstream transform can be rerun after its upstream dependency has committed.

`uv run foundry-demo run` reruns the existing pipeline. `uv run foundry-demo show` shows the current unique students. The full demo refuses to erase existing state. To rerun all phases in a new store:

```bash
export FOUNDRY_DEMO_ROOT="$PWD/.local/another-foundry-demo"
uv run foundry-demo demo
uv run foundry-demo ui
```

Use the same environment for the demo and the UI. Data is stored in `.local/foundry-demo/warehouse.sqlite` by default, proof of the runs in `demo-results.json` beside it, and run history in `.local/foundry-dagster` (or an explicit `DAGSTER_HOME`). Generated state is ignored by Git. The separate default run-history directory lets this UI coexist with the earlier orders demo.

## Verification and scope

```bash
uv run pytest -q
```

Tests execute actual Spark transformations and real Dagster jobs. They cover duplicates (including nulls), input replacement, semantic-version rebuilds, required incremental execution, snapshot reference inputs, output replacement, transaction rollback, lost commit acknowledgments, abort/reexecution, schema checks, and generated lineage. GitHub Actions runs both the Foundry-style Spark demo and the earlier orders demo on Linux with Java 17.

Persistence is intentionally small and local: JSON-encoded Spark rows in SQLite, capped at 10,000 rows per write, collected into driver memory. This is not distributed storage or a production migration runtime. Multiple outputs, Spark compute profiles, filesystem APIs, permissions/markings, and many other Foundry APIs are unsupported and must not be assumed to work. Unsupported exposed options raise errors rather than being ignored.

The next migration step is to inventory the APIs actually used by a representative private repository, extend this compatibility subset deliberately, and compare outputs against real Foundry runs. No company code, credentials, or private data belongs in this public sample.

The earlier Python/SQLite pipeline remains available as `orders-demo`; see [its walkthrough](docs/SQLITE-DEMO.md). AWS, S3/Iceberg, and distributed compute are later platform work. Separate REA research files remain local and ignored.
