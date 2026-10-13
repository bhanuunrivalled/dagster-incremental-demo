"""SQLite adapter: output, change journal, and cursor commit together per stage."""

import hashlib
import json
import sqlite3
from contextlib import contextmanager
from pathlib import Path
from typing import Callable, Iterator

from orders_demo.domain import Order, revenue_delta

FaultHook = Callable[[str], None]

SCHEMA = """
CREATE TABLE IF NOT EXISTS source_files (
    name TEXT PRIMARY KEY, sha256 TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS raw_orders (
    seq INTEGER PRIMARY KEY AUTOINCREMENT,
    event_id TEXT NOT NULL UNIQUE, payload TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS validated_orders (
    order_id TEXT PRIMARY KEY, version INTEGER NOT NULL,
    order_date TEXT NOT NULL, amount_cents INTEGER NOT NULL CHECK(amount_cents >= 0)
);
CREATE TABLE IF NOT EXISTS order_changes (
    seq INTEGER PRIMARY KEY AUTOINCREMENT,
    event_id TEXT NOT NULL UNIQUE, deltas TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS daily_revenue (
    order_date TEXT PRIMARY KEY, order_count INTEGER NOT NULL CHECK(order_count >= 0),
    revenue_cents INTEGER NOT NULL CHECK(revenue_cents >= 0)
);
CREATE TABLE IF NOT EXISTS checkpoints (
    stage TEXT PRIMARY KEY, seq INTEGER NOT NULL CHECK(seq >= 0)
);
"""


def canonical(value: object) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False)


class LocalStore:
    def __init__(self, database: Path):
        self.database = Path(database)

    @contextmanager
    def connect(self) -> Iterator[sqlite3.Connection]:
        self.database.parent.mkdir(parents=True, exist_ok=True)
        connection = sqlite3.connect(self.database, timeout=30, isolation_level=None)
        connection.row_factory = sqlite3.Row
        try:
            connection.execute("PRAGMA journal_mode=WAL")
            connection.execute("PRAGMA synchronous=FULL")
            connection.executescript(SCHEMA)
            yield connection
        finally:
            connection.close()

    @contextmanager
    def transaction(self) -> Iterator[sqlite3.Connection]:
        with self.connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            try:
                yield connection
                connection.commit()
            except BaseException:
                connection.rollback()
                raise

    @staticmethod
    def cursor(connection: sqlite3.Connection, stage: str) -> int:
        row = connection.execute("SELECT seq FROM checkpoints WHERE stage=?", (stage,)).fetchone()
        return row["seq"] if row else 0

    @staticmethod
    def checkpoint(connection: sqlite3.Connection, stage: str, seq: int) -> None:
        connection.execute(
            "INSERT INTO checkpoints VALUES (?, ?) "
            "ON CONFLICT(stage) DO UPDATE SET seq=excluded.seq",
            (stage, seq),
        )

    @staticmethod
    def fault(hook: FaultHook | None, point: str) -> None:
        if hook is not None:
            hook(point)

    def ingest(self, input_directory: Path, hook: FaultHook | None = None) -> dict:
        """Read only new immutable batches; reject edits to already ingested files."""
        if not input_directory.is_dir():
            raise ValueError(f"Input directory is missing: {input_directory}. Run orders-demo seed initial.")
        added = files = 0
        with self.transaction() as connection:
            receipts = dict(connection.execute("SELECT name, sha256 FROM source_files"))
            paths = sorted(input_directory.glob("*.json"))
            if set(receipts) - {path.name for path in paths}:
                raise ValueError("An ingested batch was removed; input batches must remain immutable")
            for path in paths:
                content = path.read_bytes()
                digest = hashlib.sha256(content).hexdigest()
                if path.name in receipts:
                    if receipts[path.name] != digest:
                        raise ValueError(f"Ingested batch changed: {path.name}; append a correction batch instead")
                    continue
                events = json.loads(content)
                if not isinstance(events, list) or not events:
                    raise ValueError(f"Batch must contain a nonempty JSON array: {path.name}")
                for event in events:
                    if not isinstance(event, dict):
                        raise ValueError("Each event must be a JSON object")
                    event_id = event.get("event_id")
                    if not isinstance(event_id, str) or not event_id.strip():
                        raise ValueError("event_id must be a nonempty string")
                    payload = canonical(event)
                    existing = connection.execute(
                        "SELECT payload FROM raw_orders WHERE event_id=?", (event_id,)
                    ).fetchone()
                    if existing is not None:
                        if existing["payload"] != payload:
                            raise ValueError(f"Conflicting reuse of event_id: {event_id}")
                        continue
                    connection.execute(
                        "INSERT INTO raw_orders(event_id, payload) VALUES (?, ?)", (event_id, payload)
                    )
                    added += 1
                self.fault(hook, "after_output")
                connection.execute("INSERT INTO source_files VALUES (?, ?)", (path.name, digest))
                files += 1
                self.fault(hook, "after_checkpoint")
            high = connection.execute("SELECT COALESCE(MAX(seq), 0) FROM raw_orders").fetchone()[0]
        self.fault(hook, "after_commit")
        return {"new_events": added, "new_files": files, "checkpoint": high}

    def validate(self, hook: FaultHook | None = None) -> dict:
        processed = changed = stale = 0
        with self.transaction() as connection:
            cursor = self.cursor(connection, "validated_orders")
            rows = connection.execute("SELECT * FROM raw_orders WHERE seq>? ORDER BY seq", (cursor,)).fetchall()
            for row in rows:
                new = Order.from_event(json.loads(row["payload"]))
                previous = connection.execute(
                    "SELECT * FROM validated_orders WHERE order_id=?", (new.order_id,)
                ).fetchone()
                old = Order(**dict(previous)) if previous else None
                if old is not None and new.version == old.version and new != old:
                    raise ValueError(f"Conflicting order version: {new.order_id} v{new.version}")
                if old is None or new.version > old.version:
                    connection.execute(
                        "INSERT INTO validated_orders VALUES (?, ?, ?, ?) "
                        "ON CONFLICT(order_id) DO UPDATE SET version=excluded.version, "
                        "order_date=excluded.order_date, amount_cents=excluded.amount_cents",
                        (new.order_id, new.version, new.order_date, new.amount_cents),
                    )
                    connection.execute(
                        "INSERT INTO order_changes(event_id, deltas) VALUES (?, ?)",
                        (row["event_id"], canonical(revenue_delta(old, new))),
                    )
                    changed += 1
                else:
                    stale += 1
                processed += 1
                cursor = row["seq"]
                self.fault(hook, "after_output")
            if rows:
                self.checkpoint(connection, "validated_orders", cursor)
                self.fault(hook, "after_checkpoint")
        self.fault(hook, "after_commit")
        return {"processed_events": processed, "changed_orders": changed, "ignored_versions": stale, "checkpoint": cursor}

    def aggregate(self, hook: FaultHook | None = None) -> dict:
        processed = 0
        affected_days: set[str] = set()
        with self.transaction() as connection:
            cursor = self.cursor(connection, "daily_revenue")
            rows = connection.execute("SELECT * FROM order_changes WHERE seq>? ORDER BY seq", (cursor,)).fetchall()
            for row in rows:
                for day, (count, cents) in json.loads(row["deltas"]).items():
                    updated = connection.execute(
                        "UPDATE daily_revenue SET order_count=order_count+?, "
                        "revenue_cents=revenue_cents+? WHERE order_date=?", (count, cents, day)
                    )
                    if updated.rowcount == 0:
                        connection.execute("INSERT INTO daily_revenue VALUES (?, ?, ?)", (day, count, cents))
                    connection.execute("DELETE FROM daily_revenue WHERE order_date=? AND order_count=0", (day,))
                    affected_days.add(day)
                cursor = row["seq"]
                processed += 1
                self.fault(hook, "after_output")
            if rows:
                self.checkpoint(connection, "daily_revenue", cursor)
                self.fault(hook, "after_checkpoint")
        self.fault(hook, "after_commit")
        return {"processed_changes": processed, "affected_days": len(affected_days), "checkpoint": cursor}

    def snapshot(self) -> dict:
        with self.connect() as connection:
            connection.execute("BEGIN")
            return {
                "raw_event_count": connection.execute("SELECT COUNT(*) FROM raw_orders").fetchone()[0],
                "orders": [dict(row) for row in connection.execute("SELECT * FROM validated_orders ORDER BY order_id")],
                "daily_revenue": [dict(row) for row in connection.execute("SELECT * FROM daily_revenue ORDER BY order_date")],
                "checkpoints": dict(connection.execute("SELECT stage, seq FROM checkpoints ORDER BY stage")),
                "change_count": connection.execute("SELECT COUNT(*) FROM order_changes").fetchone()[0],
            }
