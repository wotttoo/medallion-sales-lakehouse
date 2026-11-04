"""Test generate_data.py: du lieu sach, loi cai san, tep landing, answer key."""

import csv
import json
import os
import re
import subprocess
import sys
from pathlib import Path

import pytest

import generate_data as gd
from common.config import REPO_ROOT, load_config


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


# ---------------------------------------------------------------- T11 nhom B
def test_group_b_b1_cross_channel(cfg, dataset):
    """B1: cung order_id o 2 kenh khac nhau, updated_at khac nhau."""
    n = cfg["volume"]["orders_per_day"]
    for date in cfg["dates"]:
        copies = error_rows(dataset, date, "orders", "B1_dup_cross_channel")
        assert len(copies) == expected_count(cfg, "B1_dup_cross_channel", n)
        for c in copies:
            same = rows_of(dataset, date, "orders", c.values["order_id"])
            assert len(same) == 2
            assert same[0].channel != same[1].channel
            assert same[0].values["channel"] != same[1].values["channel"]
            assert same[0].values["updated_at"] != same[1].values["updated_at"]


def test_group_b_b2_same_updated_at(cfg, dataset):
    """B2: dung 5 cap, cung updated_at, khac status, o 2 tep khac nhau."""
    for date in cfg["dates"]:
        copies = error_rows(dataset, date, "orders", "B2_dup_same_updated_at")
        assert len(copies) == 5
        for c in copies:
            same = rows_of(dataset, date, "orders", c.values["order_id"])
            assert len(same) == 2
            a, b = same
            assert a.channel != b.channel
            assert a.values["updated_at"] == b.values["updated_at"]
            assert a.values["status"] != b.values["status"]
            assert b.values["status"] in cfg["status_weights"]


def test_group_b_sources_have_no_other_errors(cfg, dataset):
    """Cap trung B (tru ORD-1001) chi mang loi B, de minh hoa loai trung ro rang."""
    for date in cfg["dates"]:
        for r in dataset[date]["orders"]:
            if "b_source" in r.tags:
                assert [e for e in r.errors if e != "T3_schema_evolution"] == []


# ---------------------------------------------------------------- T12 nhom C
def test_group_c_c1_missing_customer(cfg, dataset):
    """C1: customer_id rong, dung ty le, gom ca ORD-1002."""
    n = cfg["volume"]["orders_per_day"]
    for date in cfg["dates"]:
        rows = error_rows(dataset, date, "orders", "C1_missing_customer_id")
        assert len(rows) == expected_count(cfg, "C1_missing_customer_id", n)
        assert all(r.to_raw(gd.ORDER_COLUMNS)["customer_id"] == "" for r in rows)
        assert all(r.clean["customer_id"] for r in rows)   # ban sach van co khach that


def test_group_c_c2_c3_items(cfg, master, dataset):
    """C2: quantity <= 0; C3: product_id khong co trong products."""
    product_ids = {p["product_id"] for p in master[cfg["dates"][0]]["products"]}
    for date in cfg["dates"]:
        n_items = len(dataset[date]["order_items"])
        c2 = error_rows(dataset, date, "order_items", "C2_quantity_non_positive")
        c3 = error_rows(dataset, date, "order_items", "C3_unknown_product")
        assert len(c2) == expected_count(cfg, "C2_quantity_non_positive", n_items)
        assert len(c3) == expected_count(cfg, "C3_unknown_product", n_items)
        assert all(r.values["quantity"] <= 0 for r in c2)
        assert all(r.values["product_id"] not in product_ids for r in c3)
        assert all(r.clean["quantity"] > 0 for r in c2)


def test_group_c_item_keys_unique(cfg, dataset):
    """Khoa (order_id, product_id) van duy nhat sau khi cai C3."""
    for date in cfg["dates"]:
        keys = [(r.values["order_id"], r.values["product_id"]) for r in dataset[date]["order_items"]]
        assert len(keys) == len(set(keys))


# ---------------------------------------------------------------- T13 nhom W
def test_group_w_w1_missing_phone(cfg, dataset):
    """W1: khoang 5% khach thieu phone; dong ghi ra co phone rong."""
    first = cfg["dates"][0]
    rows = error_rows(dataset, first, "customers", "W1_missing_phone")
    ids = {r.values["customer_id"] for r in rows}
    assert len(ids) == expected_count(cfg, "W1_missing_phone", cfg["volume"]["n_customers"])
    for date in cfg["dates"]:
        for r in error_rows(dataset, date, "customers", "W1_missing_phone"):
            assert r.to_raw(gd.CUSTOMER_COLUMNS)["phone"] == ""
        for r in dataset[date]["customers"]:
            if "W1_missing_phone" not in r.errors:
                assert r.values["phone"]


def test_group_w_w2_small_mismatch(cfg, dataset):
    """W2: lech duoi 5% va khong trung voi don co dong C2/C3."""
    n = cfg["volume"]["orders_per_day"]
    for date in cfg["dates"]:
        items_by_order = {}
        for i in dataset[date]["order_items"]:
            items_by_order.setdefault(i.values["order_id"], []).append(i)
        rows = error_rows(dataset, date, "orders", "W2_amount_mismatch")
        assert len(rows) == expected_count(cfg, "W2_amount_mismatch", n)
        for r in rows:
            items = items_by_order[r.values["order_id"]]
            assert not any(i.errors for i in items)
            items_total = sum(i.values["quantity"] * i.values["unit_price"] for i in items)
            diff = abs(r.values["total_amount"] - items_total)
            assert 0 < diff < 0.05 * items_total


def test_group_w_other_orders_match_items(cfg, dataset):
    """Don khong dinh W2 thi total_amount = tong cac dong sach."""
    for date in cfg["dates"]:
        clean_total = {}
        for i in dataset[date]["order_items"]:
            c = i.clean
            clean_total[c["order_id"]] = clean_total.get(c["order_id"], 0) + c["quantity"] * c["unit_price"]
        for r in dataset[date]["orders"]:
            if "W2_amount_mismatch" not in r.errors and "resend" not in r.tags:
                assert r.values["total_amount"] == clean_total[r.values["order_id"]]


# ---------------------------------------------------------------- T14 nhom T
def test_group_t_t1_resend(cfg, dataset):
    """T1: 10% don ngay N gui lai ngay N+1, cung kenh, updated_at moi hon, status tien len."""
    n = cfg["volume"]["orders_per_day"]
    nxt = {"pending": "shipping", "shipping": "delivered"}
    for prev, date in zip(cfg["dates"], cfg["dates"][1:]):
        rows = error_rows(dataset, date, "orders", "T1_status_update_resend")
        assert len(rows) == expected_count(cfg, "T1_status_update_resend", n)
        for r in rows:
            orig = rows_of(dataset, prev, "orders", r.values["order_id"])
            assert len(orig) == 1 and orig[0].errors == []
            o = orig[0]
            assert r.channel == o.channel
            assert r.values["updated_at"] > o.values["updated_at"]
            assert r.values["updated_at"].startswith(date)
            assert r.values["status"] == nxt[o.values["status"]]
            assert r.values["order_date"] == o.values["order_date"]


def test_group_t_t2_late(cfg, dataset):
    """T2: 3% don moi ngay N+1 co order_date = N; ngay dau khong co."""
    n = cfg["volume"]["orders_per_day"]
    assert not error_rows(dataset, cfg["dates"][0], "orders", "T2_late_arriving")
    for prev, date in zip(cfg["dates"], cfg["dates"][1:]):
        rows = error_rows(dataset, date, "orders", "T2_late_arriving")
        assert len(rows) == expected_count(cfg, "T2_late_arriving", n)
        assert all(r.values["order_date"] == prev for r in rows)
        # don den muon van co dong san pham trong tep ngay N+1
        item_orders = {i.values["order_id"] for i in dataset[date]["order_items"]}
        assert all(r.values["order_id"] in item_orders for r in rows)


def test_group_t_t3_promo_code(cfg, dataset):
    """T3: chi tep partner ngay 3 co cot promo_code."""
    day3 = cfg["dates"][2]
    for date in cfg["dates"]:
        for r in dataset[date]["orders"]:
            has = "promo_code" in r.values
            assert has == (date == day3 and r.channel == "partner")
    assert any(r.values.get("promo_code") for r in dataset[day3]["orders"])


# ---------------------------------------------------------------- T15 tep landing
@pytest.fixture(scope="module")
def landing(tmp_path_factory):
    """Chay generate_data vao thu muc tam, tra ve thu muc landing."""
    out = tmp_path_factory.mktemp("gen")
    gd.main(["--output-dir", str(out)])
    return out / "landing"


def test_landing_files_exist(cfg, landing):
    """Du 6 tep moi ngay, dung ten va vi tri."""
    for d in cfg["dates"]:
        expected = [f"{d}/orders/web_{d}.csv", f"{d}/orders/app_{d}.csv",
                    f"{d}/orders/partner_{d}.json", f"{d}/order_items_{d}.csv",
                    f"{d}/customers_{d}.csv", f"{d}/products_{d}.csv"]
        for rel in expected:
            assert (landing / rel).is_file(), rel
            assert (landing / rel).stat().st_size > 0


def test_landing_files_csv_columns(cfg, landing):
    """Header CSV dung ten, dung thu tu cot."""
    d = cfg["dates"][0]
    for rel, columns in [(f"orders/web_{d}.csv", gd.ORDER_COLUMNS),
                         (f"order_items_{d}.csv", gd.ITEM_COLUMNS),
                         (f"customers_{d}.csv", gd.CUSTOMER_COLUMNS),
                         (f"products_{d}.csv", gd.PRODUCT_COLUMNS)]:
        with (landing / d / rel).open(encoding="utf-8", newline="") as f:
            assert next(csv.reader(f)) == columns


def test_landing_files_partner_jsonl(cfg, landing):
    """Partner la JSON lines phang; ngay 3 co them promo_code."""
    for i, d in enumerate(cfg["dates"]):
        lines = (landing / d / "orders" / f"partner_{d}.json").read_text(encoding="utf-8").splitlines()
        recs = [json.loads(line) for line in lines]
        expected_cols = gd.ORDER_COLUMNS + (["promo_code"] if i == 2 else [])
        assert all(list(r) == expected_cols for r in recs)
        assert all(not isinstance(v, (dict, list)) for r in recs for v in r.values())


def test_landing_files_showcase_rows(cfg, landing):
    """Dong minh hoa dung trong tep: ORD-1001 o web va app, ORD-1002 o partner."""
    d = cfg["dates"][0]
    for ch in ["web", "app"]:
        with (landing / d / "orders" / f"{ch}_{d}.csv").open(encoding="utf-8", newline="") as f:
            first = next(csv.DictReader(f))
        assert first["order_id"] == "ORD-1001" and first["customer_id"] == "C-017"
        assert first["total_amount"] == "150000" and first["status"] == "delivered"
    rec = json.loads((landing / d / "orders" / f"partner_{d}.json").read_text(encoding="utf-8").splitlines()[0])
    assert rec["order_id"] == "ORD-1002"
    assert rec["customer_id"] == "" and rec["order_date"] == "19/10/2026"
    assert rec["total_amount"] == "150.000đ" and rec["status"] == "shipping"


def test_landing_files_row_counts(cfg, dataset, landing):
    """So dong trong tep = so LandingRow cua thuc the."""
    for d in cfg["dates"]:
        n_orders = sum(len((landing / d / "orders" / f"{ch}_{d}.{fmt}").read_text(encoding="utf-8").splitlines())
                       - (1 if fmt == "csv" else 0) for ch, fmt in cfg["channels"].items())
        assert n_orders == len(dataset[d]["orders"])
        for entity in ["order_items", "customers", "products"]:
            n = len((landing / d / f"{entity}_{d}.csv").read_text(encoding="utf-8").splitlines()) - 1
            assert n == len(dataset[d][entity])


# ---------------------------------------------------------------- T16 tat dinh
def run_generator(out_dir, hash_seed):
    """Chay generate_data.py trong tien trinh rieng voi PYTHONHASHSEED khac nhau."""
    env = dict(os.environ, PYTHONHASHSEED=str(hash_seed))
    subprocess.run([sys.executable, str(REPO_ROOT / "generate_data.py"),
                    "--output-dir", str(out_dir)], check=True, env=env,
                   cwd=str(out_dir), capture_output=True)


def tree_bytes(root):
    """dict duong dan tuong doi -> noi dung bytes cua moi tep."""
    return {p.relative_to(root).as_posix(): p.read_bytes()
            for p in sorted(Path(root).rglob("*")) if p.is_file()}


def test_deterministic_byte_identical(tmp_path):
    """Hai lan chay (khac PYTHONHASHSEED, khac thu muc) cho tep giong het tung byte."""
    a, b = tmp_path / "a", tmp_path / "b"
    a.mkdir()
    b.mkdir()
    run_generator(a, 1)
    run_generator(b, 999)
    files_a, files_b = tree_bytes(a), tree_bytes(b)
    assert files_a and list(files_a) == list(files_b)
    for rel in files_a:
        assert files_a[rel] == files_b[rel], rel
