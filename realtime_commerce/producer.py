"""Publish deterministic order events to JSONL or a Kafka-compatible broker."""

from __future__ import annotations

import argparse
import json
import os
from datetime import date
from pathlib import Path

from realtime_commerce.generator import generate_events, write_jsonl


def publish_kafka(rows: list[dict[str, object]], bootstrap: str, topic: str) -> int:
    from kafka import KafkaProducer

    producer = KafkaProducer(
        bootstrap_servers=bootstrap,
        key_serializer=lambda value: value.encode("utf-8"),
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
        acks="all",
        retries=5,
        linger_ms=25,
    )
    for row in rows:
        producer.send(topic, key=str(row["order_id"]), value=row)
    producer.flush(timeout=30)
    producer.close(timeout=10)
    return len(rows)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--date", type=date.fromisoformat, default=date.today())
    parser.add_argument("--events", type=int, default=1_000)
    parser.add_argument("--invalid-events", type=int, default=3)
    parser.add_argument("--duplicate-events", type=int, default=2)
    parser.add_argument("--seed", type=int, default=2026)
    parser.add_argument("--sink", choices=("jsonl", "kafka"), default="jsonl")
    parser.add_argument("--output", type=Path, default=Path("data/incoming/order_events.jsonl"))
    parser.add_argument(
        "--bootstrap",
        default=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:19092"),
    )
    parser.add_argument("--topic", default=os.getenv("KAFKA_TOPIC", "ecommerce.order_events"))
    args = parser.parse_args()
    rows = generate_events(
        args.date,
        events=args.events,
        invalid_events=args.invalid_events,
        duplicate_events=args.duplicate_events,
        seed=args.seed,
    )
    if args.sink == "jsonl":
        write_jsonl(args.output, rows)
        print(f"Wrote {len(rows)} events to {args.output}")
    else:
        count = publish_kafka(rows, args.bootstrap, args.topic)
        print(f"Published {count} events to {args.topic} via {args.bootstrap}")


if __name__ == "__main__":
    main()
