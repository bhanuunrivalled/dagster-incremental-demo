"""Small local Spark runtime. Dataset versions and progress commit in one database.

Rows are collected to the driver for persistence. This is an API compatibility
experiment with a 10,000-row limit per write, not a distributed data platform.
"""

import hashlib
import inspect
import json
import os
import sqlite3
import sys
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession, functions as F
from pyspark.sql.types import StructType

from transforms.api import Transform, UnsupportedFeature

MAX_ROWS = 10_000
JSON_OPTIONS = {"timestampFormat": "yyyy-MM-dd'T'HH:mm:ss.SSSSSSXXX",
                "timestampNTZFormat": "yyyy-MM-dd'T'HH:mm:ss.SSSSSS"}
SCHEMA = """
CREATE TABLE IF NOT EXISTS datasets (
    id TEXT PRIMARY KEY, schema_json TEXT NOT NULL, generation INTEGER NOT NULL,
    revision INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS batches (
    dataset TEXT NOT NULL, revision INTEGER NOT NULL, rows_json TEXT NOT NULL,
    PRIMARY KEY(dataset, revision)
);
CREATE TABLE IF NOT EXISTS receipts (
    dataset TEXT NOT NULL, batch_id TEXT NOT NULL, digest TEXT NOT NULL,
    PRIMARY KEY(dataset, batch_id)
);
CREATE TABLE IF NOT EXISTS builds (
    output TEXT PRIMARY KEY, signature TEXT NOT NULL, semantic_version INTEGER NOT NULL,
    inputs_json TEXT NOT NULL, output_revision INTEGER NOT NULL
);
"""


def local_spark():
    # Python workers must use the same virtual environment as the driver.
    os.environ["PYSPARK_PYTHON"] = sys.executable
    return (SparkSession.builder.master("local[2]").appName("foundry-style-demo")
            .config("spark.driver.bindAddress", "127.0.0.1")
            .config("spark.driver.host", "127.0.0.1")
            .config("spark.ui.enabled", "false")
            .config("spark.sql.shuffle.partitions", "2")
            .config("spark.sql.session.timeZone", "UTC").getOrCreate())


def nullable_schema(schema):
    def normalize(value):
        if isinstance(value, dict):
            return {key: True if key in {"nullable", "containsNull", "valueContainsNull"}
                    else normalize(item) for key, item in value.items()}
        if isinstance(value, list):
            return [normalize(item) for item in value]
        return value
    return json.dumps(normalize(schema.jsonValue()), sort_keys=True)


def encode(frame):
    if not isinstance(frame, DataFrame):
        raise TypeError("Expected a real PySpark DataFrame")
    if len(set(frame.columns)) != len(frame.columns):
        raise ValueError("Duplicate column names cannot be persisted")
    if not frame.columns:
        raise UnsupportedFeature("A dataset needs at least one column")
    columns = [F.col("`" + name.replace("`", "``") + "`") for name in frame.columns]
    rows = [row[0] for row in frame.limit(MAX_ROWS + 1).select(
        F.to_json(F.struct(*columns), options=JSON_OPTIONS).alias("payload")
    ).collect()]
    if len(rows) > MAX_ROWS:
        raise UnsupportedFeature(f"Local prototype limit: {MAX_ROWS} rows per write")
    return nullable_schema(frame.schema), rows


class LocalRuntime:
    def __init__(self, database: Path, spark: SparkSession):
        self.database = Path(database)
        self.spark = spark

    @contextmanager
    def transaction(self):
        self.database.parent.mkdir(parents=True, exist_ok=True)
        connection = sqlite3.connect(self.database, timeout=120, isolation_level=None)
        connection.row_factory = sqlite3.Row
        try:
            connection.execute("PRAGMA journal_mode=WAL")
            connection.execute("PRAGMA synchronous=FULL")
            connection.executescript(SCHEMA)
            connection.execute("BEGIN IMMEDIATE")
            yield connection
            connection.commit()
        except BaseException:
            connection.rollback()
            raise
        finally:
            connection.close()

    def _metadata(self, connection, dataset):
        row = connection.execute("SELECT * FROM datasets WHERE id=?", (dataset,)).fetchone()
        return dict(row) if row else None

    def _read(self, connection, dataset, after=0):
        metadata = self._metadata(connection, dataset)
        if metadata is None:
            raise ValueError(f"Missing local input dataset: {dataset}")
        payloads = [payload for row in connection.execute(
            "SELECT rows_json FROM batches WHERE dataset=? AND revision>? ORDER BY revision",
            (dataset, after)
        ) for payload in json.loads(row[0])]
        schema = StructType.fromJson(json.loads(metadata["schema_json"]))
        if not payloads:
            return self.spark.createDataFrame([], schema)
        return self.spark.read.schema(schema).options(**JSON_OPTIONS).json(
            self.spark.sparkContext.parallelize(payloads, 2)
        )

    def _write(self, connection, dataset, schema, rows, mode):
        previous = self._metadata(connection, dataset)
        if mode != "replace" and previous and previous["schema_json"] != schema:
            raise ValueError("Append schema changed; replace the source or rebuild the output")
        if previous and mode != "replace" and not rows:
            return previous["revision"]
        revision = previous["revision"] + 1 if previous else 1
        generation = (previous["generation"] if previous else 0) + int(mode == "replace" or previous is None)
        if mode == "replace":
            connection.execute("DELETE FROM batches WHERE dataset=?", (dataset,))
        connection.execute("INSERT OR REPLACE INTO datasets VALUES (?, ?, ?, ?)",
                           (dataset, schema, generation, revision))
        connection.execute("INSERT INTO batches VALUES (?, ?, ?)",
                           (dataset, revision, json.dumps(rows)))
        return revision

    def ingest(self, dataset, batch_id, frame, *, mode="append"):
        if mode not in {"append", "replace"}:
            raise ValueError("Source mode must be append or replace")
        if not isinstance(batch_id, str) or not batch_id:
            raise ValueError("Source ingestion requires a stable batch_id for retries")
        schema, rows = encode(frame)
        digest = hashlib.sha256(json.dumps([mode, schema, sorted(rows)]).encode()).hexdigest()
        with self.transaction() as connection:
            receipt = connection.execute("SELECT digest FROM receipts WHERE dataset=? AND batch_id=?",
                                         (dataset, batch_id)).fetchone()
            if receipt:
                if receipt[0] != digest:
                    raise ValueError("Batch identity was reused with different data")
                return False
            self._write(connection, dataset, schema, rows, mode)
            connection.execute("INSERT INTO receipts VALUES (?, ?, ?)", (dataset, batch_id, digest))
        return True

    def read(self, dataset):
        with self.transaction() as connection:
            return self._read(connection, dataset)

    def inspect(self):
        with self.transaction() as connection:
            return {table: [dict(row) for row in connection.execute(f"SELECT * FROM {table} ORDER BY 1, 2")]
                    for table in ("datasets", "batches", "receipts", "builds")}

    def run(self, definition: Transform, *, fault=None, force_snapshot=False):
        options = definition.incremental_options
        version = options.semantic_version if options else 1
        snapshots = set(options.snapshot_inputs if options else ())
        signature = json.dumps([definition.function.__module__, definition.function.__qualname__,
                                sorted((name, item.path) for name, item in definition.inputs.items()),
                                sorted(snapshots), definition.dataframe_function])
        with self.transaction() as connection:
            state = connection.execute("SELECT * FROM builds WHERE output=?", (definition.output.path,)).fetchone()
            output_metadata = self._metadata(connection, definition.output.path)
            current = {name: self._metadata(connection, item.path) for name, item in definition.inputs.items()}
            if any(value is None for value in current.values()):
                raise ValueError("One or more input datasets have not been ingested")
            previous = json.loads(state["inputs_json"]) if state else {}
            eligible = bool(options and state and output_metadata and
                            state["semantic_version"] == version and state["signature"] == signature and
                            state["output_revision"] == output_metadata["revision"] and not force_snapshot)
            eligible = eligible and all(name in snapshots or (
                name in previous and previous[name]["generation"] == value["generation"] and
                previous[name]["revision"] <= value["revision"]
            ) for name, value in current.items())
            if options and options.require_incremental and state and not eligible and state["semantic_version"] == version:
                raise ValueError("require_incremental=True: input/output history requires a full rebuild")
            input_objects = {name: TransformInput(self, connection, item.path,
                previous[name]["revision"] if eligible and name not in snapshots else 0,
                eligible and name not in snapshots) for name, item in definition.inputs.items()}
            output = TransformOutput(self, connection, definition.output.path, eligible)
            arguments = {name: item.dataframe() if definition.dataframe_function else item
                         for name, item in input_objects.items()}
            if not definition.dataframe_function:
                arguments[definition.output_name] = output
            if "ctx" in inspect.signature(definition.function).parameters:
                arguments["ctx"] = TransformContext(self.spark, eligible)
            result = definition.function(**arguments)
            if definition.dataframe_function:
                output.write_dataframe(result)
            elif result is not None:
                raise ValueError("@transform must write its Output and return None")
            if output.pending is None:
                raise UnsupportedFeature("Each successful transform must explicitly write its output")
            schema, rows = output.pending
            revision = self._write(connection, definition.output.path, schema, rows, output.mode)
            if fault:
                fault("after_output")
            cursors = {name: {key: value[key] for key in ("generation", "revision")}
                       for name, value in current.items()}
            connection.execute("INSERT OR REPLACE INTO builds VALUES (?, ?, ?, ?, ?)",
                               (definition.output.path, signature, version, json.dumps(cursors), revision))
            if fault:
                fault("after_checkpoint")
            report = {"dataset": definition.output.path, "is_incremental": eligible,
                      "written_rows": len(rows), "write_mode": output.mode, "output_revision": revision}
        if fault:
            fault("after_commit")
        return report


class TransformInput:
    def __init__(self, runtime, connection, path, after, incremental):
        self.runtime, self.connection, self.path = runtime, connection, path
        self.after, self.incremental = after, incremental

    def dataframe(self, mode="added"):
        if mode not in {"added", "current"}:
            raise UnsupportedFeature("Input read modes currently supported: added, current")
        return self.runtime._read(self.connection, self.path,
                                  self.after if mode == "added" and self.incremental else 0)


class TransformOutput:
    def __init__(self, runtime, connection, path, incremental):
        self.runtime, self.connection, self.path = runtime, connection, path
        self.incremental = incremental
        self.mode = "modify" if incremental else "replace"
        self.pending = None

    def dataframe(self, mode="current", schema=None):
        if mode != "previous":
            raise UnsupportedFeature("Output reads currently support only explicit mode='previous'")
        if not self.incremental:
            if not isinstance(schema, StructType):
                raise ValueError("A Spark schema is required for previous output during a full rebuild")
            return self.runtime.spark.createDataFrame([], schema)
        frame = self.runtime._read(self.connection, self.path)
        if schema is not None and frame.schema != schema:
            raise ValueError("Previous output schema differs from the supplied schema")
        return frame

    def set_mode(self, mode):
        if mode not in {"replace", "modify", "append"}:
            raise UnsupportedFeature(f"Unsupported output mode: {mode}")
        if self.pending is not None:
            raise ValueError("Output mode must be set before writing")
        if not self.incremental and mode != "replace":
            raise UnsupportedFeature("Full rebuilds must replace output in this prototype")
        self.mode = mode

    def write_dataframe(self, frame, **unsupported):
        if unsupported:
            raise UnsupportedFeature(f"Unsupported write options: {', '.join(sorted(unsupported))}")
        if self.pending is not None:
            raise UnsupportedFeature("Multiple writes in one transform are not yet supported")
        self.pending = encode(frame)


@dataclass(frozen=True)
class TransformContext:
    spark_session: SparkSession
    is_incremental: bool

    def abort_job(self):
        raise RuntimeError("Transform aborted; output and checkpoint rolled back")
