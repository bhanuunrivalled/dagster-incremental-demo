"""Business rules, independent of Dagster and persistence."""

from dataclasses import dataclass
from datetime import date
from typing import Any


@dataclass(frozen=True)
class Order:
    order_id: str
    version: int
    order_date: str
    amount_cents: int

    @classmethod
    def from_event(cls, event: dict[str, Any]) -> "Order":
        order_id = event.get("order_id")
        if not isinstance(order_id, str) or not order_id.strip():
            raise ValueError("order_id must be a nonempty string")
        version = event.get("version")
        amount = event.get("amount_cents")
        if type(version) is not int or version < 1:
            raise ValueError("version must be a positive integer")
        if type(amount) is not int or amount < 0:
            raise ValueError("amount_cents must be a nonnegative integer")
        day = event.get("order_date")
        if not isinstance(day, str) or date.fromisoformat(day).isoformat() != day:
            raise ValueError("order_date must be an ISO date YYYY-MM-DD")
        return cls(order_id, version, day, amount)


def revenue_delta(old: Order | None, new: Order) -> dict[str, tuple[int, int]]:
    """Changes to (order count, revenue cents), including moves between days."""
    deltas: dict[str, tuple[int, int]] = {}
    if old is not None:
        deltas[old.order_date] = (-1, -old.amount_cents)
    count, cents = deltas.get(new.order_date, (0, 0))
    deltas[new.order_date] = (count + 1, cents + new.amount_cents)
    return deltas
