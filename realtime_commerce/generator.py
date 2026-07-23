"""Deterministic historical and streaming order event generator."""

from __future__ import annotations

import json
import random
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Iterable

from realtime_commerce.contracts import CATEGORIES, PROVINCES


def generate_events(
    event_date: date,
    *,
    events: int = 100,
    invalid_events: int = 3,
    duplicate_events: int = 2,
    seed: int = 2026,
) -> list[dict[str, object]]:
    """Build one daily batch with late updates, cancellations, bad rows, and duplicates."""
    if invalid_events >= events:
        raise ValueError("invalid_events must be less than events")
    rng = random.Random(seed + int(event_date.strftime("%Y%m%d")))
    start = datetime.combine(event_date, datetime.min.time(), tzinfo=timezone.utc)
    rows: list[dict[str, object]] = []
    created_orders: list[str] = []

    for index in range(events):
        event_ts = start + timedelta(seconds=rng.randrange(86_400))
        can_mutate = index >= max(5, events // 4) and created_orders
        if can_mutate and rng.random() < 0.18:
            order_id = created_orders[rng.randrange(len(created_orders))]
            event_type = "cancelled" if rng.random() < 0.35 else "updated"
        else:
            order_id = f"O-{event_date:%Y%m%d}-{index:08d}"
            event_type = "created"
            created_orders.append(order_id)

        row: dict[str, object] = {
            "event_id": f"E-{event_date:%Y%m%d}-{index:09d}",
            "order_id": order_id,
            "customer_id": f"C{rng.randint(1, 12_000):07d}",
            "product_id": f"P{rng.randint(1, 2_000):06d}",
            "category": CATEGORIES[rng.randrange(len(CATEGORIES))],
            "province": PROVINCES[rng.randrange(len(PROVINCES))],
            "quantity": rng.randint(1, 5),
            "unit_price_vnd": float(rng.randrange(50_000, 5_000_001, 10_000)),
            "event_type": event_type,
            "event_ts": event_ts.isoformat(),
            "ingest_ts": (event_ts + timedelta(seconds=rng.randint(1, 120))).isoformat(),
        }
        if index < invalid_events:
            if index % 3 == 0:
                row["quantity"] = 0
            elif index % 3 == 1:
                row["province"] = "UNKNOWN"
            else:
                row["unit_price_vnd"] = -1.0
        rows.append(row)

    for index in range(min(duplicate_events, len(rows))):
        rows.append(dict(rows[invalid_events + index]))
    return rows


def write_jsonl(path: Path, rows: Iterable[dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row, separators=(",", ":")) + "\n")


def generate_history(
    incoming_root: Path,
    start_date: date,
    *,
    days: int = 35,
    events_per_day: int = 100,
    invalid_events: int = 3,
    duplicate_events: int = 2,
    seed: int = 2026,
) -> list[Path]:
    paths: list[Path] = []
    for offset in range(days):
        current = start_date + timedelta(days=offset)
        rows = generate_events(
            current,
            events=events_per_day,
            invalid_events=invalid_events,
            duplicate_events=duplicate_events,
            seed=seed,
        )
        path = incoming_root / f"event_date={current.isoformat()}" / "order_events.jsonl"
        write_jsonl(path, rows)
        paths.append(path)
    return paths
