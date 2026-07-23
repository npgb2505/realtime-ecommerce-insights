# Power BI dashboard

After `ecommerce-stream demo`, import the four files under `exports/` plus
`vietnam_provinces.csv`. Relate `province_sales[province]` and
`daily_sales[province]` to `vietnam_provinces[province]`.

## Page 1: Real-time sales pulse

```DAX
Revenue = SUM(daily_sales[revenue_vnd])
Orders = SUM(daily_sales[orders])
Units = SUM(daily_sales[units])
Average Order Value = DIVIDE([Revenue], [Orders])
```

Use cards, an hourly velocity line chart, and category/province slicers. Refresh the
CSV folder after each curated micro-batch.

## Page 2: Vietnam regional demand

Use Azure Maps or the built-in map with latitude/longitude. Bubble size is revenue;
color is average order value. Add a ranked province bar chart and category matrix.

## Page 3: Seven-day outlook

Plot historical daily revenue from `daily_sales.csv` and the seven future observations
from `sales_forecast.csv`. Clearly label the released model as a baseline linear trend
with weekday seasonality, not a production financial forecast.
