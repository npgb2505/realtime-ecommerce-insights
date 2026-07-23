from __future__ import annotations

from datetime import date
from pathlib import Path

import pytest

from realtime_commerce.generator import generate_events, generate_history
from realtime_commerce.pipeline import create_spark, curate_and_publish, land_file_stream


@pytest.fixture(scope="session")
def spark():
    session = create_spark("realtime-commerce-tests")
    yield session
    session.stop()


def test_event_generation_is_deterministic() -> None:
    first = generate_events(
        date(2025, 1, 1),
        events=20,
        invalid_events=2,
        duplicate_events=1,
    )
    second = generate_events(
        date(2025, 1, 1),
        events=20,
        invalid_events=2,
        duplicate_events=1,
    )
    assert first == second
    assert len(first) == 21


def test_stream_checkpoint_quality_and_forecast(tmp_path: Path, spark) -> None:
    project = tmp_path / "project"
    root = project / "data"
    incoming = root / "incoming"
    generate_history(
        incoming,
        date(2025, 1, 1),
        days=8,
        events_per_day=25,
        invalid_events=2,
        duplicate_events=1,
    )

    first_stream = land_file_stream(spark, incoming, root)
    second_stream = land_file_stream(spark, incoming, root)
    metrics = curate_and_publish(spark, root, export_dir=project / "exports")

    assert first_stream["rows_landed"] == 8 * 26
    assert second_stream["rows_landed"] == 0
    assert metrics["status"] == "PASS"
    assert metrics["events"]["bronze"] == 8 * 26
    assert metrics["events"]["duplicates"] == 8
    assert metrics["events"]["accepted"] + metrics["events"]["quarantined"] == 8 * 25
    assert metrics["gold"]["forecast_rows"] == 7
    assert all(metrics["assertions"].values())
    assert (project / "exports" / "province_sales.csv").exists()
    assert (project / "exports" / "sales_forecast.csv").exists()
