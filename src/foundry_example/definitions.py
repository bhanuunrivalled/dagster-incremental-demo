"""Platform entry point; the transform modules remain independent of Dagster."""

import os
from pathlib import Path

from foundry_compat.dagster_bridge import build_definitions
from foundry_example.pipeline import pipeline


def database_path():
    return Path(os.environ.get("FOUNDRY_DEMO_ROOT", Path(__file__).resolve().parents[2] / ".local" / "foundry-demo")) / "warehouse.sqlite"


defs = build_definitions(pipeline, database_path())
