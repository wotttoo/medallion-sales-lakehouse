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
