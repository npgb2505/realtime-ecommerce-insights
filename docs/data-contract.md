# Order event contract

JSON events use UTC ISO-8601 timestamps. Kafka key is `order_id`; the immutable
deduplication key is `event_id`.

| Field | Type | Rule |
|---|---|---|
| event_id | string | required and unique after latest-ingest resolution |
| order_id | string | required; groups lifecycle events |
| customer_id / product_id | string | required |
| category | string | required |
| province | string | must match the published Vietnam reference |
| quantity | integer | greater than zero |
| unit_price_vnd | decimal-like double | greater than zero; VND |
| event_type | string | created, updated, or cancelled |
| event_ts / ingest_ts | timestamp | UTC |

Invalid events remain queryable in Silver with `rejection_reason`. Compatible additive
fields may be introduced; a missing or incompatible required field must fail before
Gold publication.
