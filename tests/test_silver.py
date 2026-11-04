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


# ---------------------------------------------------------------- T34 MERGE
@pytest.fixture()
def tmp_cfg(tmp_path):
    """Config voi lakehouse tam rieng cho tung test."""
    from common.config import load_config
    return load_config(lakehouse_dir=tmp_path / "lh")


def order_batch(spark, rows, batch_id="b-0001"):
    """Lo orders da chuan hoa (v2) tu cac tuple (order_id, status, updated_at)."""
    data = [((oid, "C-1", "2026-10-19", "10", status, "web", ts), i, f"web_{batch_id}.csv")
            for i, (oid, status, ts) in enumerate(rows)]
    df = bronze_like_orders(spark, data).withColumn("_batch_id", st.F.lit(batch_id))
    return st.normalize_orders(df, "v2")


def silver_status(spark, cfg):
    """dict order_id -> status trong silver.orders."""
    return {r["order_id"]: r["status"] for r in st.read_silver(spark, cfg, "orders").collect()}


def test_merge_t1_update_and_insert(spark, tmp_cfg):
    """T1: ban moi hon cap nhat status; don moi duoc them."""
    st.merge_into_silver(spark, tmp_cfg, "orders",
                         order_batch(spark, [("ORD-1", "pending", "2026-10-19 10:00:00")]))
    v = st.merge_into_silver(spark, tmp_cfg, "orders", order_batch(spark, [
        ("ORD-1", "shipping", "2026-10-20 08:00:00"),
        ("ORD-2", "pending", "2026-10-20 09:00:00")], "b-0002"))
    assert silver_status(spark, tmp_cfg) == {"ORD-1": "shipping", "ORD-2": "pending"}
    assert v >= 2
    assert st.read_silver(spark, tmp_cfg, "orders").columns == st.silver_columns("orders")


def test_merge_older_does_not_overwrite(spark, tmp_cfg):
    """Ban cu hon khong ghi de ban moi; ban bang updated_at thi duoc cap nhat."""
    st.merge_into_silver(spark, tmp_cfg, "orders",
                         order_batch(spark, [("ORD-1", "shipping", "2026-10-20 08:00:00")]))
    st.merge_into_silver(spark, tmp_cfg, "orders",
                         order_batch(spark, [("ORD-1", "pending", "2026-10-19 10:00:00")], "b-0002"))
    assert silver_status(spark, tmp_cfg) == {"ORD-1": "shipping"}
    st.merge_into_silver(spark, tmp_cfg, "orders",
                         order_batch(spark, [("ORD-1", "delivered", "2026-10-20 08:00:00")], "b-0003"))
    assert silver_status(spark, tmp_cfg) == {"ORD-1": "delivered"}


def test_merge_rerun_same_batch_is_idempotent(spark, tmp_cfg):
    """MERGE lai cung mot lo khong them dong."""
    batch = order_batch(spark, [("ORD-1", "pending", "2026-10-19 10:00:00"),
                                ("ORD-2", "pending", "2026-10-19 11:00:00")])
    st.merge_into_silver(spark, tmp_cfg, "orders", batch)
    st.merge_into_silver(spark, tmp_cfg, "orders", batch)
    assert st.read_silver(spark, tmp_cfg, "orders").count() == 2


# ---------------------------------------------------------------- T35 quarantine
def quarantine_sample(spark, batch_id="b-0001", version="v1"):
    """Bronze mau -> _raw -> chuan hoa -> ap quy tac -> phan cach ly."""
    data = [(r, i, f"src_{i}.csv") for i, r in enumerate(SAMPLE_ORDERS)]
    bronze = bronze_like_orders(spark, data).withColumn("_batch_id", st.F.lit(batch_id))
    df = st.normalize_orders(st.attach_raw(bronze, "orders"), version)
    checked, _ = st.apply_rules(df, "orders")
    return st.split_valid_quarantine(checked)[1]


def test_quarantine_keeps_raw_strings(spark, tmp_cfg):
    """Cot goc giu nguyen string ('19/10/2026', '150.000đ') + ly do + metadata."""
    st.write_quarantine(spark, tmp_cfg, "orders", quarantine_sample(spark), "b-0001")
    q = st.read_quarantine(spark, tmp_cfg, "orders")
    assert q.columns == st.RAW_COLUMNS["orders"] + [
        "_ingest_time", "_source_file", "_batch_id", "_failed_rules", "_quarantined_at"]
    types = dict(q.dtypes)
    assert all(types[c] == "string" for c in st.RAW_COLUMNS["orders"])
    row = q.where("order_id = 'ORD-1002'").collect()[0]
    assert row["order_date"] == "19/10/2026" and row["total_amount"] == "150.000đ"
    assert row["customer_id"] == ""
    assert row["_failed_rules"] == "customer_id_not_null; order_date_valid_format; total_amount_numeric"
    assert row["_batch_id"] == "b-0001" and row["_quarantined_at"] is not None


def test_quarantine_rerun_batch_replaces(spark, tmp_cfg):
    """Ghi lai cung lo thay the phan cua lo do; lo khac giu nguyen."""
    st.write_quarantine(spark, tmp_cfg, "orders", quarantine_sample(spark, "b-0001"), "b-0001")
    st.write_quarantine(spark, tmp_cfg, "orders", quarantine_sample(spark, "b-0002"), "b-0002")
    st.write_quarantine(spark, tmp_cfg, "orders", quarantine_sample(spark, "b-0001", "v2"), "b-0001")
    q = st.read_quarantine(spark, tmp_cfg, "orders")
    counts = {r["_batch_id"]: r["count"] for r in q.groupBy("_batch_id").count().collect()}
    assert counts == {"b-0001": 2, "b-0002": 2}


# ---------------------------------------------------------------- T36 luong mot lo
@pytest.fixture(scope="module")
def generated(tmp_path_factory):
    """Sinh du lieu vao thu muc tam; tra ve (thu muc landing, answer key)."""
    import json
    import generate_data
    out = tmp_path_factory.mktemp("silver_data")
    generate_data.main(["--output-dir", str(out)])
    key = json.loads((out / "answer_key" / "answer_key.json").read_text(encoding="utf-8"))
    return out / "landing", key


def make_cfg(landing, lakehouse, version):
    """Config tro toi landing sinh san va lakehouse tam."""
    from common.config import load_config
    return load_config(landing_dir=landing, lakehouse_dir=lakehouse, rules_version=version)


def test_batch_flow_day1_matches_answer_key(spark, generated, tmp_path):
    """Lo ngay 1: thong ke tung thuc the khop answer key v1; dang thuc bao toan dung."""
    import bronze_ingest
    landing, key = generated
    cfg = make_cfg(landing, tmp_path / "lh", "v1")
    bronze_ingest.ingest_date(spark, cfg, "2026-10-19")
    assert st.pending_batches(spark, cfg) == ["b-0001"]
    result = st.process_batch(spark, cfg, "b-0001")
    expected = key["days"]["2026-10-19"]["outcomes"]["v1"]["entities"]
    assert result["entities"] == expected
    for s in result["entities"].values():
        assert s["n_input"] == s["n_valid"] + s["n_quarantined"] + s["n_duplicates_removed"]
    assert result["n_warn_by_rule"]["phone_not_null"] == \
        key["days"]["2026-10-19"]["outcomes"]["v1"]["n_warn_by_rule"]["phone_not_null"]
    assert "web_2026-10-19.csv" in result["input_files"]
    assert set(result["delta_versions_written"]) >= {"silver.orders", "quarantine.orders"}
    counts = key["days"]["2026-10-19"]["outcomes"]["v1"]["silver_counts"]
    for entity in st.ENTITY_ORDER:
        assert st.read_silver(spark, cfg, entity).count() == counts[entity]
