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
