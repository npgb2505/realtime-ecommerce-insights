# Data model

```mermaid
erDiagram
  ORDER_EVENTS ||--o{ CURRENT_ORDER_STATE : resolves_to
  CURRENT_ORDER_STATE ||--o{ DAILY_SALES : aggregates_to
  CURRENT_ORDER_STATE ||--o{ PROVINCE_SALES : aggregates_to
  CURRENT_ORDER_STATE ||--o{ SALES_VELOCITY : aggregates_to
  DAILY_SALES ||--o{ SALES_FORECAST : trains
```

Bronze grain is one event occurrence. Silver clean grain is one unique event ID;
current order state is one latest lifecycle event per order. Gold grains are
date/province/category, province, hour/province, and forecast date.
