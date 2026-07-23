"""Shared event contract and reference data."""

from pyspark.sql import types as T

ORDER_SCHEMA = T.StructType(
    [
        T.StructField("event_id", T.StringType(), False),
        T.StructField("order_id", T.StringType(), False),
        T.StructField("customer_id", T.StringType(), False),
        T.StructField("product_id", T.StringType(), False),
        T.StructField("category", T.StringType(), False),
        T.StructField("province", T.StringType(), False),
        T.StructField("quantity", T.IntegerType(), False),
        T.StructField("unit_price_vnd", T.DoubleType(), False),
        T.StructField("event_type", T.StringType(), False),
        T.StructField("event_ts", T.TimestampType(), False),
        T.StructField("ingest_ts", T.TimestampType(), False),
    ]
)

PROVINCES = (
    "Ha Noi",
    "Ho Chi Minh City",
    "Da Nang",
    "Hai Phong",
    "Can Tho",
    "Quang Ninh",
    "Thanh Hoa",
    "Nghe An",
    "Khanh Hoa",
    "Lam Dong",
    "Binh Duong",
    "Dong Nai",
)
CATEGORIES = (
    "Electronics",
    "Home",
    "Beauty",
    "Fashion",
    "Grocery",
    "Sports",
)
EVENT_TYPES = ("created", "updated", "cancelled")
