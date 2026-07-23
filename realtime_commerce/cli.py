"""CLI for local file streaming, Kafka streaming, curation, and demos."""

from __future__ import annotations

import argparse
import json
import shutil
from datetime import date
from pathlib import Path

from realtime_commerce.generator import generate_history
from realtime_commerce.pipeline import (
    create_spark,
    curate_and_publish,
    land_file_stream,
    stream_from_kafka,
)


def _clear(path: Path) -> None:
    if not path.exists():
        return
    for child in path.iterdir():
        if child.is_dir():
            shutil.rmtree(child)
        else:
            child.unlink()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)

    demo = subparsers.add_parser("demo", help="Run a deterministic available-now stream")
    demo.add_argument("--root", type=Path, default=Path("data"))
    demo.add_argument("--start-date", type=date.fromisoformat, default=date(2025, 1, 1))
    demo.add_argument("--days", type=int, default=35)
    demo.add_argument("--events-per-day", type=int, default=100)
    demo.add_argument("--invalid-events", type=int, default=3)
    demo.add_argument("--duplicate-events", type=int, default=2)
    demo.add_argument("--seed", type=int, default=2026)

    curate = subparsers.add_parser("curate", help="Rebuild Silver/Gold from Bronze")
    curate.add_argument("--root", type=Path, default=Path("data"))

    kafka = subparsers.add_parser("kafka", help="Run the long-lived Kafka stream")
    kafka.add_argument("--root", type=Path, default=Path("data"))
    kafka.add_argument("--bootstrap", default="localhost:19092")
    kafka.add_argument("--topic", default="ecommerce.order_events")
    return parser


def main() -> None:
    args = build_parser().parse_args()
    spark = create_spark()
    try:
        if args.command == "demo":
            _clear(args.root)
            _clear(Path("artifacts"))
            _clear(Path("powerbi") / "exports")
            incoming = args.root / "incoming"
            generate_history(
                incoming,
                args.start_date,
                days=args.days,
                events_per_day=args.events_per_day,
                invalid_events=args.invalid_events,
                duplicate_events=args.duplicate_events,
                seed=args.seed,
            )
            stream_result = land_file_stream(spark, incoming, args.root)
            metrics = curate_and_publish(spark, args.root)
            metrics["stream"] = stream_result
            report = Path("artifacts") / "quality-report.json"
            report.write_text(json.dumps(metrics, indent=2), encoding="utf-8")
            print(json.dumps(metrics, indent=2))
        elif args.command == "curate":
            print(json.dumps(curate_and_publish(spark, args.root), indent=2))
        else:
            stream_from_kafka(
                spark,
                args.root,
                bootstrap=args.bootstrap,
                topic=args.topic,
            )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
