# Compatibility contract for this prototype

The requirement is to preserve the team's Foundry-style authoring model while replacing the execution platform. This implementation is independent and limited; it does not install Palantir's `transforms` distribution and must not share an environment with another package exporting that namespace.

## Supported and tested subset

| Authoring feature | Local behavior |
| --- | --- |
| `Input(path)`, `Output(path)` | Logical dataset identifiers in a local catalog; no Foundry API calls |
| `@transform(...)` | One output, one or more input objects, optional `ctx`, explicit `write_dataframe` |
| `@transform_df(Output(...), ...)` | Input Spark DataFrames; return one Spark DataFrame |
| `@incremental()` | Initial/full build uses all current input and replaces output; subsequent compatible builds use added batches and default to modify |
| `semantic_version=N` | Changing this value forces a full rebuild, with empty previous output |
| `require_incremental=True` | Reject invalidated history; allow first build and explicit semantic-version change |
| `snapshot_inputs=[...]` | Named reference inputs always read their current snapshot and can be replaced without invalidating the other inputs' incrementality |
| Input `.dataframe()` / `added` / `current` | Real Spark DataFrames for the appropriate input window |
| Output `.dataframe('previous', schema=...)` | Committed previous output in incremental mode; empty typed frame in a full build; supplied schemas must match the stored nullable schema |
| Output `.write_dataframe(df)` | One write per invocation; persist output and all input cursors in one SQLite transaction |
| Output `.set_mode('replace')` | Replace the output even during an incremental build; `modify` and `append` add rows |
| `ctx.is_incremental`, `ctx.spark_session`, `ctx.abort_job()` | Report execution mode, expose actual Spark, or fail and roll back the local transaction |
| `Pipeline.add_transforms(...)`, `.discover_transforms(module)` | Register transforms or recursively import a package; detect duplicate outputs and dependency cycles |
| Input/output declarations | Generate Dagster dataset dependencies without changing the author functions |

Function bodies use ordinary PySpark, including filters, joins, grouping, distinct, and subtraction. The sample's deduplication compute body matches the published example; dataset paths and the surrounding environment are synthetic. The companion counts transform demonstrates `transform_df`.

These tests validate our implementation against explicit expected results, not against a running Foundry tenant. There is no blanket assertion of behavior parity, source compatibility for an entire company repository, or compatibility with every Foundry library version.

## State and failure rules

Source ingestion requires a stable batch ID. Identical redelivery is ignored; conflicting reuse fails. Each input has a revision and a generation. Appending advances its revision; replacing advances its generation. A transform tracks both. Replaced non-reference input, changed bindings, or externally changed output causes a full rebuild (or rejection when incremental execution is required). Code changes require the author to bump `semantic_version` when they change incremental meaning; source code is not automatically analyzed for this.

All output rows and input cursors commit together under a SQLite write transaction. Failure before commit rolls both back. A retry after a lost acknowledgment reads the committed cursor; the tested incremental deduplication example writes no duplicates. Arbitrary external side effects inside user functions cannot be made transactional by this adapter. Each transform is its own transaction, not the whole pipeline.

Spark work runs with a local driver and two local worker threads. Data is serialized as Spark JSON with its schema, including microsecond timestamp formatting; schemas are stored with nullable fields. All pending rows and reads fit in driver memory. Each write is limited to 10,000 rows, but accumulated dataset history can exceed that and is still read into memory. Do not use this runtime for large datasets.

## Unsupported features

- Multiple outputs, filesystem/media APIs, Pandas/Polars/lightweight transforms, streaming, Ontology APIs, secret/credential bindings, dataset checks, markings, permissions, and Foundry branch semantics.
- `@configure` compute profiles: rejected explicitly until there is a real mapping to a compute backend.
- Retention/delete flags, strict-append configuration, v2 semantics, and other unimplemented incremental decorator options.
- Input `previous` mode; output `current`/`added` reads; partitioning/bucketing/write options; multiple writes per output per invocation.
- Automatic schema evolution, arbitrary Spark version compatibility, distributed storage, worker provisioning, and production scheduling or access control.

Options on exposed methods are rejected if unsupported. Unknown imports fail to import; unknown methods fail on access. That makes missing support visible, but cannot detect every semantic difference in a company's code. Evaluate representative transforms and their build history before claiming migration readiness. Existing pipelines should stay in a private workspace; keep this public repository synthetic.

## Reference behavior

- [Palantir incremental examples](https://www.palantir.com/docs/foundry/transforms-python-spark/incremental-examples)
- [Palantir incremental usage and read/write modes](https://www.palantir.com/docs/foundry/transforms-python/incremental-usage)
- [Palantir pipeline discovery](https://www.palantir.com/docs/foundry/transforms-python/pipelines)
- [Apache Spark 3.5.7 runtime requirements](https://spark.apache.org/docs/3.5.7/)
