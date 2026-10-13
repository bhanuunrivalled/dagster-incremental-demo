"""Operator commands for the small local compatibility demonstration."""

import argparse
import json
import os
import sys
from pathlib import Path

import dagster as dg
from pyspark.sql.types import StringType, StructField, StructType

from foundry_compat.runtime import LocalRuntime, local_spark
from foundry_example.definitions import database_path, defs
from orders_demo.cli import prepare_home

STUDENT_SCHEMA = StructType([StructField("student_id", StringType()), StructField("hair", StringType())])


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=["init", "demo", "show", "run", "ui"])
    parser.add_argument("--port", type=int, default=3000)
    args = parser.parse_args()
    os.environ.setdefault("DAGSTER_HOME", str(Path(__file__).resolve().parents[2] / ".local" / "foundry-dagster"))
    prepare_home()
    if args.command == "init":
        print("Local run history is ready")
        return
    if args.command == "ui":
        os.execv(sys.executable, [sys.executable, "-m", "dagster", "dev", "--host", "127.0.0.1",
                                 "--port", str(args.port), "-m", "foundry_example.definitions"])
    spark = local_spark()
    spark.sparkContext.setLogLevel("ERROR")
    runtime = LocalRuntime(database_path(), spark)
    try:
        if args.command == "show":
            print(runtime.read("/demo/unique_students").orderBy("student_id").toJSON().collect())
            return
        if args.command == "demo" and runtime.inspect()["datasets"]:
            parser.error("demo needs a fresh FOUNDRY_DEMO_ROOT; use run to repeat existing data")
        phases = [("initial", [("A", "Brown"), ("A", "Brown"), ("B", "Black")], "append", 2),
                  ("new", [("B", "Black"), ("C", "Brown")], "append", 3),
                  ("repeat", None, "append", 3),
                  ("source_replaced", [("D", "Blonde")], "replace", 1)]
        if args.command == "run":
            phases = [("manual", None, "append", None)]
        reports = []
        with dg.DagsterInstance.get() as instance:
            for phase, rows, mode, expected in phases:
                if rows is not None:
                    runtime.ingest("/demo/students", phase, spark.createDataFrame(rows, STUDENT_SCHEMA), mode=mode)
                result = defs.resolve_job_def("foundry_pipeline").execute_in_process(
                    instance=instance, resources={"foundry_runtime": runtime}, tags={"demo/phase": phase}
                )
                unique = runtime.read("/demo/unique_students").orderBy("student_id").toJSON().collect()
                if expected is not None:
                    assert len(unique) == expected, (phase, unique)
                reports.append({"phase": phase, "run_id": result.run_id, "unique_students": unique,
                                "assets": [{"asset": event.asset_key.to_user_string(),
                                            "metadata": {key: value.value for key, value in event.materialization.metadata.items()}}
                                           for event in result.get_asset_materialization_events()]})
        report_path = database_path().parent / "demo-results.json"
        report_path.write_text(json.dumps(reports, indent=2) + "\n")
        print(json.dumps(reports, indent=2))
        print(f"Verified Foundry-style Spark demo: {report_path}")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
