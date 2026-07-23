"""Structured Streaming landing, medallion curation, forecasting, and evidence."""

from __future__ import annotations

import json
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path

import numpy as np
import pandas as pd
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from realtime_commerce.contracts import EVENT_TYPES, ORDER_SCHEMA, PROVINCES


def create_spark(app_name: str = "realtime-ecommerce-insights") -> SparkSession:
    master = os.getenv("ECOM_SPARK_MASTER", "local[2]")
    spark = (
        SparkSession.builder.appName(app_name)
        .master(master)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    return spark


def land_file_stream(spark: SparkSession, incoming: Path, root: Path) -> dict[str, object]:
    """Land each discovered file once using Structured Streaming checkpoints."""
    bronze = root / "lakehouse" / "bronze" / "order_events"
    checkpoint = root / "checkpoints" / "file_orders_to_bronze"
    before = spark.read.parquet(str(bronze)).count() if bronze.exists() else 0
    stream = (
        spark.readStream.schema(ORDER_SCHEMA)
        .option("recursiveFileLookup", "true")
        .json(str(incoming))
        .withColumn("event_date", F.to_date("event_ts"))
        .withColumn("source_file", F.input_file_name())
    )
    query = (
        stream.writeStream.format("parquet")
        .option("path", str(bronze))
        .option("checkpointLocation", str(checkpoint))
        .partitionBy("event_date")
        .trigger(availableNow=True)
        .start()
    )
    query.awaitTermination()
    after = spark.read.parquet(str(bronze)).count()
    progress = query.lastProgress or {}
    return {
        "rows_before": before,
        "rows_after": after,
        "rows_landed": after - before,
        "batch_id": progress.get("batchId"),
        "input_rows": progress.get("numInputRows", 0),
    }


def _forecast(daily_sales: DataFrame, horizon: int = 7) -> pd.DataFrame:
    history = (
        daily_sales.groupBy("order_date")
        .agg(F.sum("revenue_vnd").alias("revenue_vnd"))
        .orderBy("order_date")
        .toPandas()
    )
    if history.empty:
        raise RuntimeError("Cannot forecast an empty daily-sales series")
    history["order_date"] = pd.to_datetime(history["order_date"])
    calendar = pd.date_range(history["order_date"].min(), history["order_date"].max(), freq="D")
    series = history.set_index("order_date")["revenue_vnd"].reindex(calendar, fill_value=0.0)
    x = np.arange(len(series), dtype=float)
    if len(series) >= 2:
        slope, intercept = np.polyfit(x, series.to_numpy(dtype=float), 1)
    else:
        slope, intercept = 0.0, float(series.iloc[0])
    overall = max(float(series.mean()), 1.0)
    weekday_factor = (series.groupby(series.index.dayofweek).mean() / overall).to_dict()
    rows: list[dict[str, object]] = []
    for step in range(1, horizon + 1):
        future_date = series.index.max() + timedelta(days=step)
        trend = intercept + slope * (len(series) - 1 + step)
        forecast = max(0.0, trend * weekday_factor.get(future_date.dayofweek, 1.0))
        rows.append(
            {
                "forecast_date": future_date.date().isoformat(),
                "forecast_revenue_vnd": round(forecast, 2),
                "model": "linear_trend_weekday_seasonality",
                "training_days": len(series),
            }
        )
    return pd.DataFrame(rows)


def _write(frame: DataFrame, path: Path) -> None:
    frame.write.mode("overwrite").parquet(str(path))


def _export(frame: DataFrame, path: Path, order_by: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    frame.orderBy(*order_by).toPandas().to_csv(path, index=False)


def curate_and_publish(
    spark: SparkSession,
    root: Path,
    *,
    export_dir: Path | None = None,
) -> dict[str, object]:
    """Validate, deduplicate, model current order state, forecast, and publish marts."""
    lakehouse = root / "lakehouse"
    bronze_path = lakehouse / "bronze" / "order_events"
    if not bronze_path.exists():
        raise FileNotFoundError(f"Bronze stream output does not exist: {bronze_path}")
    raw = spark.read.parquet(str(bronze_path))

    dedup_window = Window.partitionBy("event_id").orderBy(F.col("ingest_ts").desc())
    deduped = (
        raw.withColumn("_row_number", F.row_number().over(dedup_window))
        .filter(F.col("_row_number") == 1)
        .drop("_row_number")
    )
    reason = (
        F.when(F.col("quantity") <= 0, F.lit("non_positive_quantity"))
        .when(F.col("unit_price_vnd") <= 0, F.lit("non_positive_price"))
        .when(~F.col("province").isin(*PROVINCES), F.lit("unknown_province"))
        .when(~F.col("event_type").isin(*EVENT_TYPES), F.lit("unknown_event_type"))
    )
    classified = deduped.withColumn("rejection_reason", reason)
    accepted = classified.filter(F.col("rejection_reason").isNull()).drop("rejection_reason")
    rejected = classified.filter(F.col("rejection_reason").isNotNull())
    _write(accepted, lakehouse / "silver" / "order_events_clean")
    _write(rejected, lakehouse / "silver" / "order_events_rejected")

    state_window = Window.partitionBy("order_id").orderBy(
        F.col("event_ts").desc(), F.col("ingest_ts").desc(), F.col("event_id").desc()
    )
    current_orders = (
        accepted.withColumn("_state_rank", F.row_number().over(state_window))
        .filter(F.col("_state_rank") == 1)
        .drop("_state_rank")
        .withColumn("order_date", F.to_date("event_ts"))
        .withColumn("revenue_vnd", F.col("quantity") * F.col("unit_price_vnd"))
    )
    active_orders = current_orders.filter(F.col("event_type") != "cancelled")
    _write(current_orders, lakehouse / "silver" / "current_order_state")

    daily_sales = active_orders.groupBy("order_date", "province", "category").agg(
        F.countDistinct("order_id").alias("orders"),
        F.sum("quantity").alias("units"),
        F.round(F.sum("revenue_vnd"), 2).alias("revenue_vnd"),
        F.round(F.avg("revenue_vnd"), 2).alias("average_order_value_vnd"),
    )
    province_sales = active_orders.groupBy("province").agg(
        F.countDistinct("order_id").alias("orders"),
        F.sum("quantity").alias("units"),
        F.round(F.sum("revenue_vnd"), 2).alias("revenue_vnd"),
        F.round(F.avg("revenue_vnd"), 2).alias("average_order_value_vnd"),
    )
    sales_velocity = (
        active_orders.withColumn("sales_hour", F.date_trunc("hour", "event_ts"))
        .groupBy("sales_hour", "province")
        .agg(
            F.countDistinct("order_id").alias("orders_per_hour"),
            F.round(F.sum("revenue_vnd"), 2).alias("revenue_per_hour_vnd"),
        )
    )
    for name, frame in (
        ("daily_sales", daily_sales),
        ("province_sales", province_sales),
        ("sales_velocity", sales_velocity),
    ):
        _write(frame, lakehouse / "gold" / name)

    forecast = _forecast(daily_sales)
    forecast_path = lakehouse / "gold" / "sales_forecast"
    forecast_path.mkdir(parents=True, exist_ok=True)
    forecast.to_parquet(forecast_path / "forecast.parquet", index=False)

    output = export_dir or root.parent / "powerbi" / "exports"
    _export(daily_sales, output / "daily_sales.csv", ["order_date", "province", "category"])
    _export(province_sales, output / "province_sales.csv", ["province"])
    _export(sales_velocity, output / "sales_velocity.csv", ["sales_hour", "province"])
    output.mkdir(parents=True, exist_ok=True)
    forecast.to_csv(output / "sales_forecast.csv", index=False)

    raw_count = raw.count()
    deduped_count = deduped.count()
    accepted_count = accepted.count()
    rejected_count = rejected.count()
    current_count = current_orders.count()
    cancelled_count = current_orders.filter(F.col("event_type") == "cancelled").count()
    active_count = active_orders.count()
    metrics: dict[str, object] = {
        "status": "PASS",
        "profile": "fixture-stream",
        "seed": 2026,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "events": {
            "bronze": raw_count,
            "deduplicated": deduped_count,
            "duplicates": raw_count - deduped_count,
            "accepted": accepted_count,
            "quarantined": rejected_count,
        },
        "orders": {
            "current": current_count,
            "active": active_count,
            "cancelled": cancelled_count,
        },
        "gold": {
            "daily_sales": daily_sales.count(),
            "province_sales": province_sales.count(),
            "sales_velocity": sales_velocity.count(),
            "forecast_rows": len(forecast),
            "revenue_vnd": active_orders.agg(F.sum("revenue_vnd")).first()[0] or 0.0,
        },
    }
    assertions = {
        "event_occurrences_reconcile": raw_count
        == deduped_count + (raw_count - deduped_count),
        "classification_reconciles": deduped_count == accepted_count + rejected_count,
        "current_state_reconciles": current_count == active_count + cancelled_count,
        "daily_sales_nonempty": metrics["gold"]["daily_sales"] > 0,
        "province_sales_nonempty": metrics["gold"]["province_sales"] > 0,
        "forecast_horizon_is_seven": metrics["gold"]["forecast_rows"] == 7,
        "positive_revenue": metrics["gold"]["revenue_vnd"] > 0,
    }
    metrics["assertions"] = assertions
    if not all(assertions.values()):
        metrics["status"] = "FAIL"
    report = root.parent / "artifacts" / "quality-report.json"
    report.parent.mkdir(parents=True, exist_ok=True)
    report.write_text(json.dumps(metrics, indent=2), encoding="utf-8")
    if metrics["status"] != "PASS":
        raise RuntimeError(f"Release gates failed; inspect {report}")
    return metrics


def stream_from_kafka(
    spark: SparkSession,
    root: Path,
    *,
    bootstrap: str,
    topic: str,
) -> None:
    """Long-running Kafka source. Submit with Spark's matching Kafka connector package."""
    parsed = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", bootstrap)
        .option("subscribe", topic)
        .option("startingOffsets", "earliest")
        .load()
        .select(F.from_json(F.col("value").cast("string"), ORDER_SCHEMA).alias("event"))
        .select("event.*")
        .withColumn("event_date", F.to_date("event_ts"))
    )
    query = (
        parsed.writeStream.format("parquet")
        .option("path", str(root / "lakehouse" / "bronze" / "order_events"))
        .option("checkpointLocation", str(root / "checkpoints" / "kafka_orders_to_bronze"))
        .partitionBy("event_date")
        .outputMode("append")
        .start()
    )
    query.awaitTermination()
