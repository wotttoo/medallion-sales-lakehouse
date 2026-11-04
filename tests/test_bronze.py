"""Test Bronze: kiem tra tep, doc string, append + bo qua tep da nap, doi chieu answer key."""

import json

import pytest

import bronze_ingest as bi
import generate_data
from common.config import load_config


@pytest.fixture(scope="module")
def generated(tmp_path_factory):
    """Sinh du lieu vao thu muc tam; tra ve (thu muc goc, answer key)."""
    out = tmp_path_factory.mktemp("bronze_data")
    generate_data.main(["--output-dir", str(out)])
    key = json.loads((out / "answer_key" / "answer_key.json").read_text(encoding="utf-8"))
    return out, key


@pytest.fixture(scope="module")
def cfg(generated, tmp_path_factory):
    """Config tro toi landing da sinh va lakehouse tam (dung chung trong module)."""
    out, _ = generated
    return load_config(landing_dir=out / "landing", lakehouse_dir=tmp_path_factory.mktemp("lh"))


# ---------------------------------------------------------------- T22 kiem tra tep
def test_file_checks_listing_order(cfg):
    """Thu tu co dinh; orders theo web -> app -> partner."""
    files = bi.list_landing_files(cfg, "2026-10-19")
    assert [f["file_name"] for f in files] == [
        "products_2026-10-19.csv", "customers_2026-10-19.csv",
        "web_2026-10-19.csv", "app_2026-10-19.csv", "partner_2026-10-19.json",
        "order_items_2026-10-19.csv"]
    assert all(f["path"].is_file() for f in files)


def test_file_checks_valid_files_return_row_counts(cfg, generated):
    """Tep hop le tra ve so dong khop answer key."""
    _, key = generated
    for f in bi.list_landing_files(cfg, "2026-10-19"):
        assert bi.check_file(f) == key["days"]["2026-10-19"]["landing_file_counts"][f["file_name"]]


def make_info(path, entity="orders", fmt="csv"):
    """Thong tin tep tu duong dan."""
    return {"entity": entity, "file_name": path.name, "fmt": fmt, "path": path,
            "file_date": "2026-10-19"}


@pytest.mark.parametrize("content, fmt, message", [
    (None, "csv", "Khong tim thay"),
    ("", "csv", "rong"),
    ("order_id,customer_id,order_date,total_amount,status,channel,updated_at\n", "csv",
     "khong co dong"),
    ("order_id,customer_id\nORD-1,C-1\n", "csv", "thieu cot"),
    ("order_id,customer_id,order_date,total_amount,status,channel,updated_at\nORD-1,C-1\n",
     "csv", "cot"),
    ('{"order_id": "ORD-1",\n', "json", "JSON loi"),
])
def test_file_checks_bad_files_name_the_file(tmp_path, content, fmt, message):
    """Tep loi thi raise va thong bao co ten tep."""
    path = tmp_path / f"bad_2026-10-19.{fmt}"
    if content is not None:
        path.write_text(content, encoding="utf-8")
    with pytest.raises(bi.FileCheckError) as exc:
        bi.check_file(make_info(path, fmt=fmt))
    assert path.name in str(exc.value)
    assert message in str(exc.value)


# ---------------------------------------------------------------- T23 doc string
def test_read_as_string_all_columns_string(spark, cfg):
    """CSV va JSON deu doc moi cot la string, dung thu tu cot, co metadata."""
    files = {f["file_name"]: f for f in bi.list_landing_files(cfg, "2026-10-19")}
    t = bi.next_ingest_time()
    for name in ["web_2026-10-19.csv", "partner_2026-10-19.json", "order_items_2026-10-19.csv"]:
        df = bi.read_file_as_strings(spark, files[name], t)
        entity = files[name]["entity"]
        assert df.columns == bi.SOURCE_COLUMNS[entity] + ["_ingest_time", "_source_file"]
        types = dict(df.dtypes)
        assert all(types[c] == "string" for c in bi.SOURCE_COLUMNS[entity])
        assert types["_ingest_time"] == "timestamp"
        assert {r["_source_file"] for r in df.select("_source_file").collect()} == {name}


def test_read_as_string_keeps_raw_values(spark, cfg):
    """Gia tri goc giu nguyen: ORD-1002 co '19/10/2026', '150.000đ'."""
    f = next(x for x in bi.list_landing_files(cfg, "2026-10-19")
             if x["file_name"] == "partner_2026-10-19.json")
    row = bi.read_file_as_strings(spark, f, bi.next_ingest_time()) \
        .where("order_id = 'ORD-1002'").collect()[0]
    assert row["order_date"] == "19/10/2026"
    assert row["total_amount"] == "150.000đ"
    assert row["customer_id"] == ""


def test_read_as_string_promo_code_day3(spark, cfg):
    """Tep partner ngay 3 co them cot promo_code (dat sau cot chuan)."""
    f = next(x for x in bi.list_landing_files(cfg, "2026-10-21") if x["fmt"] == "json")
    df = bi.read_file_as_strings(spark, f, bi.next_ingest_time())
    assert df.columns[:8] == bi.SOURCE_COLUMNS["orders"] + ["promo_code"]


def test_read_as_string_ingest_time_increases():
    """Gio nap tang ngat qua tung tep."""
    t1 = bi.next_ingest_time()
    t2 = bi.next_ingest_time(t1)
    t3 = bi.next_ingest_time(t2 + bi.timedelta(seconds=5))
    assert t1 < t2 < t3
