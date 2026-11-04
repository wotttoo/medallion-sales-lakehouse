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


# ---------------------------------------------------------------- T30 orders v2
def test_normalize_orders_v2_parses_reading_formats(spark):
    """v2: '19/10/2026' -> 2026-10-19; '150.000đ' -> 150000."""
    import datetime as dt
    rows = normalized(spark, "v2")
    r = rows["ORD-1002"]
    assert r["order_date"] == dt.date(2026, 10, 19)
    assert r["total_amount"] == 150000
    assert r["customer_id"] is None          # v2 van khong sua duoc customer_id rong
    assert rows["ORD-1001"]["total_amount"] == 150000


def test_normalize_orders_v2_still_rejects_garbage(spark):
    """v2 khong chap nhan ngay khong ton tai hay so tien sai dang."""
    rows = normalized(spark, "v2")
    assert rows["ORD-1004"]["order_date"] is None
    assert rows["ORD-1004"]["total_amount"] is None
    df = spark.createDataFrame(
        [("X", "C", "31/02/2026", "1.50đ", "pending", "web", "2026-10-19 00:00:00"),
         ("Y", "C", "19/10/2026", "1.234.567đ", "pending", "web", "2026-10-19 00:00:00")], ORDER_COLS)
    got = {r["order_id"]: r for r in st.normalize_orders(df, "v2").collect()}
    assert got["X"]["order_date"] is None and got["X"]["total_amount"] is None
    assert got["Y"]["total_amount"] == 1234567


# ---------------------------------------------------------------- T31 order_items
def test_normalize_items_line_amount(spark):
    """Ep kieu va line_amount = quantity * unit_price; quantity sai -> NULL."""
    df = spark.createDataFrame([("ORD-1001", "P-001", "3", "50000"),
                                ("ORD-1005", "P-002", "-1", "75000"),
                                ("ORD-1006", "P-003", "x", "1000")],
                               "order_id string, product_id string, quantity string, unit_price string")
    out = st.normalize_order_items(df)
    assert out.columns[:5] == ["order_id", "product_id", "quantity", "unit_price", "line_amount"]
    types = dict(out.dtypes)
    assert types["quantity"] == "int" and types["unit_price"] == "bigint"
    assert types["line_amount"] == "bigint"
    rows = {r["order_id"]: r for r in out.collect()}
    assert rows["ORD-1001"]["line_amount"] == 150000
    assert rows["ORD-1005"]["quantity"] == -1 and rows["ORD-1005"]["line_amount"] == -75000
    assert rows["ORD-1006"]["quantity"] is None and rows["ORD-1006"]["line_amount"] is None


def test_normalize_items_dispatch(spark):
    """normalize() chon dung ham theo thuc the."""
    with pytest.raises(ValueError):
        st.normalize(None, "unknown", "v1")


# ---------------------------------------------------------------- T32 ap quy tac
def checked_orders(spark, version):
    """Chuan hoa + ap quy tac cho SAMPLE_ORDERS."""
    df = st.normalize_orders(spark.createDataFrame(SAMPLE_ORDERS, ORDER_COLS), version)
    return st.apply_rules(df, "orders")


def test_apply_rules_failed_rules_all_evaluated(spark):
    """Danh gia tat ca quy tac, khong dung o quy tac dau; dung thu tu khai bao."""
    checked, _ = checked_orders(spark, "v1")
    got = {r["order_id"]: r["_failed_rules"] for r in checked.collect()}
    assert got["ORD-1001"] is None
    assert got["ORD-1002"] == "customer_id_not_null; order_date_valid_format; total_amount_numeric"
    assert got["ORD-1004"] == "order_date_valid_format; total_amount_numeric"
    checked, _ = checked_orders(spark, "v2")
    got = {r["order_id"]: r["_failed_rules"] for r in checked.collect()}
    assert got["ORD-1002"] == "customer_id_not_null"


def test_apply_rules_split(spark):
    """Hop le khong con cot _failed_rules; quarantine giu ly do."""
    checked, _ = checked_orders(spark, "v1")
    valid, quarantined = st.split_valid_quarantine(checked)
    assert sorted(r["order_id"] for r in valid.collect()) == ["ORD-1001", "ORD-1003"]
    assert "_failed_rules" not in valid.columns
    assert sorted(r["order_id"] for r in quarantined.collect()) == ["ORD-1002", "ORD-1004"]


def test_apply_rules_status_and_warn(spark):
    """status ngoai tap cho phep bi drop; phone_not_null chi dem canh bao."""
    df = spark.createDataFrame(
        [("ORD-9", "C-1", "2026-10-19", "1", "lost", "web", "2026-10-19 00:00:00")], ORDER_COLS)
    checked, warns = st.apply_rules(st.normalize_orders(df, "v2"), "orders")
    assert checked.collect()[0]["_failed_rules"] == "status_in_allowed_set"
    assert warns == {}
    cust = st.normalize_customers(spark.createDataFrame(
        [("C-1", "A", "a@x.com", "", "Hue"), ("C-2", "B", "b@x.com", "0900000000", "Hue")],
        CUSTOMER_COLS))
    checked, warns = st.apply_rules(cust, "customers")
    assert warns == {"phone_not_null": 1}
    valid, quarantined = st.split_valid_quarantine(checked)
    assert valid.count() == 2 and quarantined.count() == 0


def test_apply_rules_items_reference_flags(spark):
    """order_items: quantity_positive, product_exists, order_exists qua cot co tu join."""
    items = st.normalize_order_items(spark.createDataFrame(
        [("ORD-1", "P-001", "2", "10"), ("ORD-1", "P-999", "0", "10"), ("ORD-2", "P-001", "1", "10")],
        "order_id string, product_id string, quantity string, unit_price string"))
    products = spark.createDataFrame([("P-001",)], "product_id string")
    orders = spark.createDataFrame([("ORD-1",)], "order_id string")
    items = st.add_exists_flag(items, products, "product_id", "_product_exists")
    items = st.add_exists_flag(items, orders, "order_id", "_order_exists")
    checked, _ = st.apply_rules(items, "order_items")
    got = {(r["order_id"], r["product_id"]): r["_failed_rules"] for r in checked.collect()}
    assert got == {("ORD-1", "P-001"): None,
                   ("ORD-1", "P-999"): "quantity_positive; product_exists",
                   ("ORD-2", "P-001"): "order_exists"}


# ---------------------------------------------------------------- T33 loai trung
def bronze_like_orders(spark, rows):
    """Tao DataFrame giong Bronze (co metadata) tu list (row, ingest_time, source_file)."""
    import datetime as dt
    data = [r + (dt.datetime(2026, 10, 19, 12, 0, 0) + dt.timedelta(seconds=t), f)
            for r, t, f in rows]
    return spark.createDataFrame(data, ORDER_COLS + ", _ingest_time timestamp, _source_file string")


def test_dedup_latest_updated_at_wins(spark):
    """B1: dong co updated_at moi hon duoc giu, bat ke thu tu tep."""
    df = bronze_like_orders(spark, [
        (("ORD-1", "C-1", "2026-10-19", "10", "pending", "web", "2026-10-19 10:00:00"), 1, "web.csv"),
        (("ORD-1", "C-1", "2026-10-19", "10", "pending", "app", "2026-10-19 09:00:00"), 2, "app.csv"),
    ])
    out = st.deduplicate(st.normalize_orders(df, "v2"), "orders").collect()
    assert [(r["order_id"], r["channel"]) for r in out] == [("ORD-1", "web")]


def test_dedup_tie_breaker_b2(spark):
    """B2: cung updated_at -> _ingest_time muon hon thang; tiep theo la _source_file."""
    same = "2026-10-19 10:00:00"
    df = bronze_like_orders(spark, [
        (("ORD-2", "C-1", "2026-10-19", "10", "pending", "web", same), 1, "web.csv"),
        (("ORD-2", "C-1", "2026-10-19", "10", "shipping", "partner", same), 3, "partner.json"),
        (("ORD-3", "C-1", "2026-10-19", "10", "pending", "web", same), 5, "a.csv"),
        (("ORD-3", "C-1", "2026-10-19", "10", "delivered", "app", same), 5, "b.csv"),
    ])
    for _ in range(3):   # ket qua on dinh qua nhieu lan chay
        got = {r["order_id"]: r["status"]
               for r in st.deduplicate(st.normalize_orders(df.repartition(4), "v2"), "orders").collect()}
        assert got == {"ORD-2": "shipping", "ORD-3": "delivered"}


def test_dedup_entities_without_updated_at(spark):
    """customers: khong co updated_at, sap theo _ingest_time roi _source_file."""
    import datetime as dt
    t = dt.datetime(2026, 10, 19, 12, 0, 0)
    df = spark.createDataFrame(
        [("C-1", "A", "a@x.com", "1", "Hue", t, "c1.csv"),
         ("C-1", "A", "a@x.com", "1", "Da Nang", t + dt.timedelta(seconds=1), "c2.csv"),
         ("C-2", "B", "B@X.COM", "1", "Hue", t, "c1.csv"),
         ("C-2", "B", "b@x.com", "1", "Hue", t, "c1.csv")],
        CUSTOMER_COLS + ", _ingest_time timestamp, _source_file string")
    out = {r["customer_id"]: r for r in st.deduplicate(st.normalize_customers(df), "customers").collect()}
    assert out["C-1"]["city"] == "Da Nang"
    assert out["C-2"]["email"] == "b@x.com" and len(out) == 2
