import json
import os
import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor

import dagster as dg
import pytest

from orders_demo.cli import seed
from orders_demo.definitions import OrdersStorage, defs
from orders_demo.store import LocalStore


@pytest.fixture
def warehouse(tmp_path):
    store = LocalStore(tmp_path / "warehouse.sqlite")
    seed("initial", tmp_path)
    store.ingest(tmp_path / "input")
    store.validate()
    store.aggregate()
    return store, tmp_path


def append(root, events, name="04-extra.json"):
    (root / "input" / name).write_text(json.dumps(events), encoding="utf-8")


def event(**overrides):
    return {"event_id": "extra-005", "order_id": "ORD-005", "version": 1,
            "order_date": "2026-10-14", "amount_cents": 4000, **overrides}


def totals(store):
    return {row["order_date"]: (row["order_count"], row["revenue_cents"]) for row in store.snapshot()["daily_revenue"]}


def execute(root, instance):
    return defs.resolve_job_def("orders_pipeline").execute_in_process(
        instance=instance, resources={"storage": OrdersStorage(root=str(root))}, raise_on_error=False
    )


def test_actual_dagster_discovery_and_four_phase_pipeline(tmp_path):
    dg.Definitions.validate_loadable(defs)
    graph = defs.resolve_asset_graph()
    assert {key.to_user_string() for key in graph.get_all_asset_keys()} == {
        "raw_orders", "validated_orders", "daily_revenue"
    }
    assert graph.get(dg.AssetKey("validated_orders")).parent_keys == {dg.AssetKey("raw_orders")}
    assert graph.get(dg.AssetKey("daily_revenue")).parent_keys == {dg.AssetKey("validated_orders")}
    store = LocalStore(tmp_path / "warehouse.sqlite")
    with dg.DagsterInstance.ephemeral() as instance:
        seed("initial", tmp_path)
        first = execute(tmp_path, instance)
        assert first.success
        assert len(first.get_asset_materialization_events()) == 3
        assert totals(store) == {"2026-10-12": (2, 20000), "2026-10-13": (1, 5000)}

        seed("new", tmp_path)
        second = execute(tmp_path, instance)
        assert second.success
        assert totals(store) == {"2026-10-12": (2, 20000), "2026-10-13": (2, 12500)}
        assert next(e.materialization.metadata["new_events"].value for e in second.get_asset_materialization_events() if e.asset_key == dg.AssetKey("raw_orders")) == 1

        seed("correction", tmp_path)
        assert execute(tmp_path, instance).success
        assert totals(store) == {"2026-10-12": (1, 12000), "2026-10-13": (3, 21500)}
        snapshot = store.snapshot()
        repeat = execute(tmp_path, instance)
        assert repeat.success
        assert store.snapshot() == snapshot
        assert snapshot["raw_event_count"] == snapshot["change_count"] == 5
        assert len(snapshot["orders"]) == 4
        metadata = {e.asset_key.to_user_string(): e.materialization.metadata for e in repeat.get_asset_materialization_events()}
        assert metadata["raw_orders"]["new_events"].value == 0
        assert metadata["validated_orders"]["processed_events"].value == 0
        assert metadata["daily_revenue"]["processed_changes"].value == 0


@pytest.mark.parametrize("stage", ["ingest", "validate", "aggregate"])
@pytest.mark.parametrize("point", ["after_output", "after_checkpoint"])
def test_output_and_checkpoint_roll_back_together_then_retry(warehouse, stage, point):
    store, root = warehouse
    seed("new", root)
    if stage != "ingest":
        store.ingest(root / "input")
    if stage == "aggregate":
        store.validate()
    before = store.snapshot()

    def fail(here):
        if here == point:
            raise RuntimeError("injected failure before commit")

    def call(hook=None):
        return store.ingest(root / "input", hook) if stage == "ingest" else getattr(store, stage)(hook)

    with pytest.raises(RuntimeError, match="injected"):
        call(fail)
    assert store.snapshot() == before
    call()
    committed = store.snapshot()
    assert committed != before
    call()
    assert store.snapshot() == committed
    store.validate()
    store.aggregate()
    assert totals(store) == {"2026-10-12": (2, 20000), "2026-10-13": (2, 12500)}


@pytest.mark.parametrize("stage", ["ingest", "validate", "aggregate"])
def test_commit_succeeds_but_acknowledgment_fails_retry_is_noop(warehouse, stage):
    store, root = warehouse
    seed("new", root)
    if stage != "ingest":
        store.ingest(root / "input")
    if stage == "aggregate":
        store.validate()

    def lose_ack(point):
        if point == "after_commit":
            raise RuntimeError("lost acknowledgment")

    with pytest.raises(RuntimeError, match="acknowledgment"):
        if stage == "ingest":
            store.ingest(root / "input", lose_ack)
        else:
            getattr(store, stage)(lose_ack)
    committed = store.snapshot()
    result = store.ingest(root / "input") if stage == "ingest" else getattr(store, stage)()
    assert store.snapshot() == committed
    assert result.get("new_events", result.get("processed_events", result.get("processed_changes"))) == 0


def test_dagster_failed_aggregate_then_successful_reexecution(warehouse, monkeypatch):
    store, root = warehouse
    seed("correction", root)
    original = LocalStore.aggregate
    failed = False

    def aggregate_once_failing(self, hook=None):
        nonlocal failed
        if not failed:
            failed = True

            def fault(point):
                if point == "after_checkpoint":
                    raise RuntimeError("simulated aggregate interruption")

            return original(self, fault)
        return original(self, hook)

    monkeypatch.setattr(LocalStore, "aggregate", aggregate_once_failing)
    with dg.DagsterInstance.ephemeral() as instance:
        assert not execute(root, instance).success
        assert totals(store) == {"2026-10-12": (2, 20000), "2026-10-13": (1, 5000)}
        assert store.snapshot()["checkpoints"] == {"daily_revenue": 3, "validated_orders": 4}
        assert execute(root, instance).success
        assert totals(store) == {"2026-10-12": (1, 12000), "2026-10-13": (2, 14000)}
        assert store.snapshot()["change_count"] == 4


def test_duplicate_delivery_and_out_of_order_versions_do_not_double_count(warehouse):
    store, root = warehouse
    initial = json.loads((root / "input" / "01-initial.json").read_text())[0]
    append(root, [initial, event(event_id="latest", order_id="ORD-001", version=3, amount_cents=15000),
                  event(event_id="late", order_id="ORD-001", version=2, amount_cents=13000),
                  event(event_id="equal", order_id="ORD-001", version=3, amount_cents=15000)])
    assert store.ingest(root / "input")["new_events"] == 3
    assert store.validate()["changed_orders"] == 1
    assert store.aggregate()["processed_changes"] == 1
    assert totals(store) == {"2026-10-12": (1, 8000), "2026-10-13": (1, 5000), "2026-10-14": (1, 15000)}


def test_conflicting_event_id_does_not_commit_partial_ingest(warehouse):
    store, root = warehouse
    before = store.snapshot()
    append(root, [event(), event(event_id="initial-001")])
    with pytest.raises(ValueError, match="Conflicting reuse"):
        store.ingest(root / "input")
    assert store.snapshot() == before


@pytest.mark.parametrize("bad", [event(amount_cents=-1), event(order_date="not-a-date"), event(amount_cents=True),
                                  event(event_id="conflict", order_id="ORD-001", version=1, amount_cents=1)])
def test_invalid_batch_rolls_back_validation_and_does_not_advance_cursor(warehouse, bad):
    store, root = warehouse
    append(root, [event(), bad])
    # Use distinct delivery identities so validation, rather than ingestion, rejects the batch.
    records = json.loads((root / "input" / "04-extra.json").read_text())
    records[1]["event_id"] = "invalid-second"
    append(root, records)
    store.ingest(root / "input")
    before = store.snapshot()
    with pytest.raises(ValueError):
        store.validate()
    assert store.snapshot() == before
    assert store.aggregate()["processed_changes"] == 0


def test_immutable_batch_edit_is_rejected(warehouse):
    store, root = warehouse
    before = store.snapshot()
    (root / "input" / "01-initial.json").write_text("[]")
    with pytest.raises(ValueError, match="Ingested batch changed"):
        store.ingest(root / "input")
    assert store.snapshot() == before


def test_correction_removes_empty_old_day_bucket(tmp_path):
    (tmp_path / "input").mkdir()
    append(tmp_path, [event(order_date="2026-10-12")], "01.json")
    store = LocalStore(tmp_path / "warehouse.sqlite")
    store.ingest(tmp_path / "input")
    store.validate()
    store.aggregate()
    append(tmp_path, [event(event_id="move", version=2, order_date="2026-10-13", amount_cents=6500)], "02.json")
    store.ingest(tmp_path / "input")
    store.validate()
    store.aggregate()
    assert totals(store) == {"2026-10-13": (1, 6500)}


def test_overlapping_consumers_apply_changes_once(warehouse):
    store, root = warehouse
    seed("correction", root)
    store.ingest(root / "input")
    store.validate()
    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(lambda _: LocalStore(store.database).aggregate(), range(2)))
    assert sorted(result["processed_changes"] for result in results) == [0, 1]
    assert totals(store) == {"2026-10-12": (1, 12000), "2026-10-13": (2, 14000)}


def test_cli_demo_persists_four_real_runs_and_rejects_destructive_replay(tmp_path):
    environment = {**os.environ, "ORDERS_DEMO_ROOT": str(tmp_path / "data"), "DAGSTER_HOME": str(tmp_path / "dagster")}
    command = [sys.executable, "-m", "orders_demo.cli", "demo"]
    result = subprocess.run(command, env=environment, capture_output=True, text=True, timeout=60)
    assert result.returncode == 0, result.stderr
    reports = json.loads((tmp_path / "data" / "demo-results.json").read_text())
    assert [report["phase"] for report in reports] == ["initial", "new", "correction", "repeat"]
    assert reports[-1]["warehouse"] == reports[-2]["warehouse"]
    assert len({report["run_id"] for report in reports}) == 4
    again = subprocess.run(command, env=environment, capture_output=True, text=True, timeout=60)
    assert again.returncode != 0
    assert "fresh ORDERS_DEMO_ROOT" in again.stderr
