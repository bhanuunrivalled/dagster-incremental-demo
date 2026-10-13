"""Dagster orchestration; the business logic and SQLite adapter are separate."""

import os
from pathlib import Path

import dagster as dg

from orders_demo.store import LocalStore


def demo_root() -> Path:
    return Path(os.environ.get("ORDERS_DEMO_ROOT", Path(__file__).resolve().parents[2] / ".local" / "orders-demo"))


class OrdersStorage(dg.ConfigurableResource):
    root: str

    def store(self) -> LocalStore:
        return LocalStore(Path(self.root) / "warehouse.sqlite")


def report(context: dg.AssetExecutionContext, storage: OrdersStorage, metrics: dict, table: str) -> dg.MaterializeResult:
    context.log.info("%s incremental result: %s", table, metrics)
    snapshot = storage.store().snapshot()
    rows = snapshot["raw_event_count"] if table == "raw_orders" else len(snapshot["orders"] if table == "validated_orders" else snapshot["daily_revenue"])
    metadata = {**metrics, "dagster/row_count": rows, "database": str(storage.store().database), "table": table}
    if table == "daily_revenue":
        metadata["revenue_cents"] = sum(row["revenue_cents"] for row in snapshot["daily_revenue"])
        metadata["preview"] = dg.MetadataValue.md(
            "| Day | Orders | Revenue (EUR) |\n| --- | ---: | ---: |\n" + "\n".join(
                f"| {row['order_date']} | {row['order_count']} | {row['revenue_cents'] / 100:.2f} |"
                for row in snapshot["daily_revenue"]
            )
        )
    return dg.MaterializeResult(metadata=metadata)


@dg.asset(group_name="orders", kinds={"python", "sqlite"}, description="Append immutable synthetic order events, deduplicated by event_id.")
def raw_orders(context: dg.AssetExecutionContext, storage: OrdersStorage) -> dg.MaterializeResult:
    return report(context, storage, storage.store().ingest(Path(storage.root) / "input"), "raw_orders")


@dg.asset(deps=[raw_orders], group_name="orders", kinds={"python", "sqlite"}, description="Validate new events and upsert only newer order versions; emit correction deltas atomically.")
def validated_orders(context: dg.AssetExecutionContext, storage: OrdersStorage) -> dg.MaterializeResult:
    return report(context, storage, storage.store().validate(), "validated_orders")


@dg.asset(deps=[validated_orders], group_name="orders", kinds={"python", "sqlite"}, description="Apply only unconsumed deltas to daily totals, subtracting an order's previous contribution on correction.")
def daily_revenue(context: dg.AssetExecutionContext, storage: OrdersStorage) -> dg.MaterializeResult:
    return report(context, storage, storage.store().aggregate(), "daily_revenue")


orders_job = dg.define_asset_job("orders_pipeline", selection=dg.AssetSelection.groups("orders"), executor_def=dg.in_process_executor)
defs = dg.Definitions(
    assets=[raw_orders, validated_orders, daily_revenue],
    jobs=[orders_job],
    resources={"storage": OrdersStorage(root=str(demo_root()))},
)
