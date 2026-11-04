"""Test generate_data.py: du lieu sach, loi cai san, tep landing, answer key."""

import re

import pytest

import generate_data as gd
from common.config import load_config


@pytest.fixture(scope="module")
def cfg():
    """Config mac dinh."""
    return load_config()


@pytest.fixture(scope="module")
def master(cfg):
    """Products/customers theo ngay."""
    return gd.make_master_data(cfg)


# ---------------------------------------------------------------- T07 master
def test_master_products_snapshot(cfg, master):
    """Ngay 1 co du san pham, dung cot, ID dang P-001."""
    products = master[cfg["dates"][0]]["products"]
    assert len(products) == cfg["volume"]["n_products"]
    assert [p["product_id"] for p in products] == \
        [f"P-{i:03d}" for i in range(1, cfg["volume"]["n_products"] + 1)]
    for p in products:
        assert list(p) == gd.PRODUCT_COLUMNS
        assert p["category"] in cfg["volume"]["categories"]
        assert isinstance(p["list_price"], int) and p["list_price"] > 0


def test_master_customers_snapshot(cfg, master):
    """Ngay 1 co du khach, ID dang C-001, email/phone hop le."""
    customers = master[cfg["dates"][0]]["customers"]
    assert len(customers) == cfg["volume"]["n_customers"]
    ids = [c["customer_id"] for c in customers]
    assert ids[0] == "C-001" and len(set(ids)) == len(ids)
    for c in customers:
        assert list(c) == gd.CUSTOMER_COLUMNS
        assert re.fullmatch(r"C-\d{3}", c["customer_id"])
        assert c["email"] == c["email"].lower() and "@" in c["email"]
        assert re.fullmatch(r"09\d{8}", c["phone"])


def test_master_later_days_are_incremental(cfg, master):
    """Cac ngay sau chi gom khach moi/cap nhat va san pham doi gia."""
    vol = cfg["volume"]
    seen = {c["customer_id"] for c in master[cfg["dates"][0]]["customers"]}
    for date in cfg["dates"][1:]:
        day = master[date]
        assert len(day["products"]) == vol["updated_products_per_day"]
        assert len(day["customers"]) == vol["new_customers_per_day"] + vol["updated_customers_per_day"]
        new_ids = {c["customer_id"] for c in day["customers"]} - seen
        assert len(new_ids) == vol["new_customers_per_day"]
        seen |= new_ids


def test_master_is_seeded(cfg):
    """Cung seed cho cung ket qua."""
    assert gd.make_master_data(cfg) == gd.make_master_data(cfg)


def test_master_showcase_product_prices(cfg, master):
    """P-001, P-002 co gia co dinh cho ban ghi minh hoa."""
    prices = {p["product_id"]: p["list_price"] for p in master[cfg["dates"][0]]["products"]}
    assert prices["P-001"] == 50_000 and prices["P-002"] == 75_000


# ---------------------------------------------------------------- T08 clean orders
@pytest.fixture(scope="module")
def clean_orders(cfg, master):
    """Don sach theo ngay."""
    return gd.make_clean_orders(cfg, master)


def test_clean_orders_ids_sequential(cfg, clean_orders):
    """order_id tang dan lien tuc tu ORD-1001 qua cac ngay."""
    ids = [o["order"]["order_id"] for d in cfg["dates"] for o in clean_orders[d]]
    n = cfg["volume"]["orders_per_day"] * len(cfg["dates"])
    assert ids == [f"ORD-{i}" for i in range(1001, 1001 + n)]


def test_clean_orders_totals_and_items(cfg, master, clean_orders):
    """total_amount = tong quantity*unit_price; khong lap san pham trong don."""
    lo, hi = cfg["volume"]["items_per_order"]
    for date in cfg["dates"]:
        for o in clean_orders[date]:
            order, items = o["order"], o["items"]
            assert list(order) == gd.ORDER_COLUMNS
            assert lo <= len(items) <= hi
            assert len({i["product_id"] for i in items}) == len(items)
            assert all(list(i) == gd.ITEM_COLUMNS for i in items)
            assert order["total_amount"] == sum(i["quantity"] * i["unit_price"] for i in items)
            assert order["order_date"] == date
            assert order["updated_at"].startswith(date)
            assert order["status"] in cfg["status_weights"]
            assert order["channel"] in cfg["channels"]


def test_clean_orders_reference_known_customers(cfg, master, clean_orders):
    """Don chi tro toi khach da co den ngay do."""
    known = set()
    for date in cfg["dates"]:
        known |= {c["customer_id"] for c in master[date]["customers"]}
        assert {o["order"]["customer_id"] for o in clean_orders[date]} <= known


# ---------------------------------------------------------------- T09 showcase
@pytest.fixture(scope="module")
def dataset(cfg):
    """Bo du lieu day du (dong landing + loi)."""
    return gd.build_dataset(cfg)


def rows_of(dataset, date, entity, order_id=None):
    """Loc dong theo order_id."""
    rows = dataset[date][entity]
    if order_id is None:
        return rows
    return [r for r in rows if r.values.get("order_id") == order_id]


def test_showcase_ord_1001(cfg, dataset):
    """ORD-1001: C-017, 150000, delivered, co o ca web va app."""
    day1 = cfg["dates"][0]
    rows = rows_of(dataset, day1, "orders", "ORD-1001")
    assert sorted(r.channel for r in rows) == ["app", "web"]
    web = next(r for r in rows if r.channel == "web")
    app = next(r for r in rows if r.channel == "app")
    for r in rows:
        raw = r.to_raw(gd.ORDER_COLUMNS)
        assert raw["customer_id"] == "C-017"
        assert raw["order_date"] == "2026-10-19"
        assert raw["total_amount"] == "150000"
        assert raw["status"] == "delivered"
    assert app.values["updated_at"] > web.values["updated_at"]
    assert "B1_dup_cross_channel" in app.errors
    items = rows_of(dataset, day1, "order_items", "ORD-1001")
    assert sum(i.values["quantity"] * i.values["unit_price"] for i in items) == 150_000


def test_showcase_ord_1002(cfg, dataset):
    """ORD-1002: partner, customer_id rong, 19/10/2026, 150.000đ, shipping."""
    day1 = cfg["dates"][0]
    rows = rows_of(dataset, day1, "orders", "ORD-1002")
    assert len(rows) == 1
    r = rows[0]
    assert r.channel == "partner"
    raw = r.to_raw(gd.ORDER_COLUMNS)
    assert raw["customer_id"] == ""
    assert raw["order_date"] == "19/10/2026"
    assert raw["total_amount"] == "150.000đ"
    assert raw["status"] == "shipping"
    assert {"C1_missing_customer_id", "A1_date_ddmmyyyy", "A2_amount_vnd_string"} <= set(r.errors)
    items = rows_of(dataset, day1, "order_items", "ORD-1002")
    assert sum(i.values["quantity"] * i.values["unit_price"] for i in items) == 150_000


def test_showcase_clean_values_kept(cfg, dataset):
    """Ban sach cua ORD-1002 van duoc luu de answer key dung."""
    r = rows_of(dataset, cfg["dates"][0], "orders", "ORD-1002")[0]
    assert r.clean["order_date"] == "2026-10-19"
    assert r.clean["total_amount"] == 150_000


# ---------------------------------------------------------------- T10 nhom A
def error_rows(dataset, date, entity, code):
    """Cac dong cua ngay co ma loi code."""
    return [r for r in dataset[date][entity] if code in r.errors]


def expected_count(cfg, code, n):
    """So loi mong doi theo ty le config."""
    return gd.count_for(cfg["error_rates"][code], n)


def test_group_a_counts_match_rates(cfg, dataset):
    """A1, A2, A3 dung so luong theo ty le tren don moi trong ngay."""
    n = cfg["volume"]["orders_per_day"]
    for date in cfg["dates"]:
        for code in ["A1_date_ddmmyyyy", "A2_amount_vnd_string", "A3_status_case"]:
            assert len(error_rows(dataset, date, "orders", code)) == expected_count(cfg, code, n), (date, code)


def test_group_a_a1_mostly_partner(cfg, dataset):
    """A1 chu yeu tu kenh partner."""
    for date in cfg["dates"]:
        rows = error_rows(dataset, date, "orders", "A1_date_ddmmyyyy")
        share = sum(r.channel == "partner" for r in rows) / len(rows)
        assert share >= 0.7


def test_group_a_raw_formats(cfg, dataset):
    """Chuoi ghi ra dung dang loi."""
    for date in cfg["dates"]:
        for r in error_rows(dataset, date, "orders", "A1_date_ddmmyyyy"):
            assert re.fullmatch(r"\d{2}/\d{2}/\d{4}", r.to_raw(gd.ORDER_COLUMNS)["order_date"])
        for r in error_rows(dataset, date, "orders", "A2_amount_vnd_string"):
            raw = r.to_raw(gd.ORDER_COLUMNS)["total_amount"]
            assert re.fullmatch(r"\d{1,3}(\.\d{3})*đ", raw)
            assert int(raw[:-1].replace(".", "")) == r.values["total_amount"]
        for r in error_rows(dataset, date, "orders", "A3_status_case"):
            raw = r.to_raw(gd.ORDER_COLUMNS)["status"]
            assert raw != r.values["status"] and raw.lower() == r.values["status"]


def test_group_a_a4_email_case(cfg, dataset):
    """A4: cung customer_id 2 dong, email chi khac chu hoa thuong."""
    for date in cfg["dates"]:
        customers = dataset[date]["customers"]
        copies = error_rows(dataset, date, "customers", "A4_email_case")
        assert len(copies) == expected_count(cfg, "A4_email_case",
                                             len(customers) - len(copies))
        for c in copies:
            same = [r for r in customers if r.values["customer_id"] == c.values["customer_id"]]
            assert len(same) == 2
            emails = [r.to_raw(gd.CUSTOMER_COLUMNS)["email"] for r in same]
            assert emails[0] != emails[1] and emails[0].lower() == emails[1].lower()
