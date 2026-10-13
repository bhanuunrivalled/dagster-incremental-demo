"""Reproducible fixtures and execution through the real Dagster job."""

import argparse
import json
import os
import tempfile
from pathlib import Path

import dagster as dg

from orders_demo.definitions import defs, demo_root
from orders_demo.store import LocalStore

PROJECT = Path(__file__).resolve().parents[2]
BATCHES = {"initial": "01-initial.json", "new": "02-new.json", "correction": "03-correction.json"}


def prepare_home() -> Path:
    home = Path(os.environ.get("DAGSTER_HOME", PROJECT / ".local" / "dagster")).resolve()
    home.mkdir(parents=True, exist_ok=True)
    configuration = home / "dagster.yaml"
    if not configuration.exists():
        configuration.write_text("telemetry:\n  enabled: false\n", encoding="utf-8")
    os.environ["DAGSTER_HOME"] = str(home)
    return home


def seed(batch: str, root: Path | None = None) -> Path:
    root = root or demo_root()
    destination = root / "input" / BATCHES[batch]
    destination.parent.mkdir(parents=True, exist_ok=True)
    data = (PROJECT / "sample_data" / BATCHES[batch]).read_bytes()
    # Publish a complete immutable batch; a repeat seed may only reuse identical bytes.
    with tempfile.NamedTemporaryFile(dir=destination.parent, prefix=".batch-", delete=False) as output:
        temporary = Path(output.name)
        output.write(data)
        output.flush()
        os.fsync(output.fileno())
    try:
        try:
            os.link(temporary, destination)
        except FileExistsError:
            if destination.read_bytes() != data:
                raise ValueError(f"Existing batch differs; refusing to overwrite {destination}")
    finally:
        temporary.unlink()
    return destination


def run(label: str) -> dict:
    prepare_home()
    with dg.DagsterInstance.get() as instance:
        result = defs.resolve_job_def("orders_pipeline").execute_in_process(instance=instance, tags={"demo/phase": label})
        if not result.success:
            raise RuntimeError(f"Dagster run failed: {result.run_id}")
        summary = {
            "phase": label,
            "run_id": result.run_id,
            "assets": {
                event.asset_key.to_user_string(): {
                    key: value.value for key, value in event.materialization.metadata.items()
                    if key != "preview"
                }
                for event in result.get_asset_materialization_events()
            },
            "warehouse": LocalStore(demo_root() / "warehouse.sqlite").snapshot(),
        }
    print(json.dumps(summary, indent=2))
    return summary


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser("init", help="Prepare a local Dagster home with telemetry disabled")
    seed_parser = commands.add_parser("seed", help="Append one immutable synthetic input batch")
    seed_parser.add_argument("batch", choices=BATCHES)
    run_parser = commands.add_parser("run", help="Execute the three-asset Dagster job")
    run_parser.add_argument("--label", default="manual")
    commands.add_parser("show", help="Inspect persistent output and checkpoints")
    commands.add_parser("demo", help="Run initial, new, correction, and repeat phases in order")
    args = parser.parse_args()
    if args.command == "init":
        print(prepare_home())
    elif args.command == "seed":
        print(seed(args.batch))
    elif args.command == "run":
        run(args.label)
    elif args.command == "show":
        print(json.dumps(LocalStore(demo_root() / "warehouse.sqlite").snapshot(), indent=2))
    elif args.command == "demo":
        store = LocalStore(demo_root() / "warehouse.sqlite")
        if store.database.exists() and store.snapshot()["raw_event_count"]:
            parser.error("demo requires a fresh ORDERS_DEMO_ROOT; use seed/run to continue existing data")
        reports = []
        for batch in BATCHES:
            seed(batch)
            reports.append(run(batch))
        reports.append(run("repeat"))
        before, after = reports[-2]["warehouse"], reports[-1]["warehouse"]
        if before != after:
            raise AssertionError("Repeat run changed the warehouse")
        proof = demo_root() / "demo-results.json"
        proof.write_text(json.dumps(reports, indent=2) + "\n", encoding="utf-8")
        print(f"Verified repeat run is unchanged. Full results: {proof}")


if __name__ == "__main__":
    main()
