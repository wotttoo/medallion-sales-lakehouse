"""Test Silver: chuan hoa, ap quy tac, loai trung, MERGE, quarantine, doi chieu answer key."""

import pytest

import silver_transform as st

PRODUCT_COLS = "product_id string, name string, category string, list_price string"
CUSTOMER_COLS = "customer_id string, name string, email string, phone string, city string"


# ---------------------------------------------------------------- T28 chuan hoa master
def test_normalize_master_products(spark):
    """Trim, ep list_price ve long; gia khong hop le -> NULL."""
    df = spark.createDataFrame([(" P-001 ", " Sach 001 ", "Books ", "50000"),
                                ("P-002", "Son", "Beauty", "abc")], PRODUCT_COLS)
    rows = {r["product_id"]: r for r in st.normalize_products(df).collect()}
    assert rows["P-001"]["name"] == "Sach 001" and rows["P-001"]["category"] == "Books"
    assert rows["P-001"]["list_price"] == 50000
    assert rows["P-002"]["list_price"] is None
    assert dict(st.normalize_products(df).dtypes)["list_price"] == "bigint"


def test_normalize_master_customers(spark):
    """Email lowercase + trim; phone rong -> NULL."""
    df = spark.createDataFrame([("C-001", "An", " An.Nguyen001@EXAMPLE.COM ", "", "Hue"),
                                ("C-002", "Binh", "binh@example.com", None, " Da Nang "),
                                ("C-003", "Chi", "chi@example.com", "0901234567", "Hue")],
                               CUSTOMER_COLS)
    rows = {r["customer_id"]: r for r in st.normalize_customers(df).collect()}
    assert rows["C-001"]["email"] == "an.nguyen001@example.com"
    assert rows["C-001"]["phone"] is None
    assert rows["C-002"]["phone"] is None and rows["C-002"]["city"] == "Da Nang"
    assert rows["C-003"]["phone"] == "0901234567"


def test_normalize_master_keeps_metadata(spark):
    """Cot metadata Bronze duoc giu lai."""
    df = spark.createDataFrame([("P-1", "a", "Books", "1", "f.csv", "b-0001")],
                               PRODUCT_COLS + ", _source_file string, _batch_id string")
    assert st.normalize_products(df).columns == ["product_id", "name", "category", "list_price",
                                                 "_source_file", "_batch_id"]


# ---------------------------------------------------------------- T29 orders v1
ORDER_COLS = ("order_id string, customer_id string, order_date string, total_amount string, "
              "status string, channel string, updated_at string")
SAMPLE_ORDERS = [
    ("ORD-1001", "C-017", "2026-10-19", "150000", "delivered", "web", "2026-10-19 09:15:00"),
    ("ORD-1002", "", "19/10/2026", "150.000đ", "shipping", "partner", "2026-10-19 10:05:00"),
    ("ORD-1003", " C-001 ", " 2026-10-19 ", "410000", "DELIVERED", "Web", "2026-10-19 08:02:24"),
    ("ORD-1004", "C-002", "2026-02-30", "12a", "Pending", "app", "bad"),
]


def normalized(spark, version):
    """Chuan hoa SAMPLE_ORDERS, tra ve dict order_id -> Row."""
    df = spark.createDataFrame(SAMPLE_ORDERS, ORDER_COLS)
    return {r["order_id"]: r for r in st.normalize_orders(df, version).collect()}


def test_normalize_orders_v1_types(spark):
    """Kieu sau chuan hoa: date, long, timestamp; them promo_code null neu thieu."""
    df = st.normalize_orders(spark.createDataFrame(SAMPLE_ORDERS, ORDER_COLS), "v1")
    types = dict(df.dtypes)
    assert types["order_date"] == "date" and types["total_amount"] == "bigint"
    assert types["updated_at"] == "timestamp" and types["promo_code"] == "string"


def test_normalize_orders_v1_values(spark):
    """v1: chi YYYY-MM-DD va so thuan; status/channel lowercase; chuoi rong -> NULL."""
    import datetime as dt
    rows = normalized(spark, "v1")
    r = rows["ORD-1001"]
    assert r["order_date"] == dt.date(2026, 10, 19) and r["total_amount"] == 150000
    assert r["status"] == "delivered" and r["updated_at"] == dt.datetime(2026, 10, 19, 9, 15)
    r = rows["ORD-1002"]
    assert r["customer_id"] is None
    assert r["order_date"] is None and r["total_amount"] is None   # v1 khong hieu
    r = rows["ORD-1003"]
    assert r["customer_id"] == "C-001" and r["order_date"] == dt.date(2026, 10, 19)
    assert r["status"] == "delivered" and r["channel"] == "web"
    r = rows["ORD-1004"]
    assert r["order_date"] is None and r["total_amount"] is None
    assert r["status"] == "pending" and r["updated_at"] is None
