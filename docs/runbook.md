# Runbook

## Broker-free verified demo

```bash
docker compose run --rm pipeline demo --root /app/data
```

Structured Streaming discovers JSONL files, lands each file exactly once using the
checkpoint, and then publishes Silver/Gold data products.

## Kafka path

```bash
docker compose up -d redpanda console
docker compose --profile kafka run --rm producer
```

Submit the long-running consumer with Spark's matching Kafka connector:

```bash
spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.0 \
  scripts/run_kafka_stream.py kafka \
  --root data --bootstrap localhost:19092
```

The command above illustrates connector coordinates; use `python -m
realtime_commerce.cli kafka` only when the connector is already on Spark's classpath.

## Recovery and replay

Do not delete `data/checkpoints` during normal recovery. Restart the same stream and
Spark resumes from committed offsets/files. To intentionally replay all inputs into a
fresh environment, move the existing `data/` directory aside or run `demo`, which
clears only project-generated children.

## Data-quality triage

Inspect `data/lakehouse/silver/order_events_rejected`, group by rejection reason,
correct the producer/contract, and publish a new event ID. Never edit Bronze records.
