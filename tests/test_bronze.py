"""Test Bronze: kiem tra tep, doc string, append + bo qua tep da nap, doi chieu answer key."""

import json

import pytest

import bronze_ingest as bi
import generate_data
from common import run_log
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


# ---------------------------------------------------------------- T24 append + skip
@pytest.fixture()
def fresh_cfg(generated, tmp_path):
    """Config voi lakehouse rieng cho tung test."""
    out, _ = generated
    return load_config(landing_dir=out / "landing", lakehouse_dir=tmp_path / "lh")


def bronze_counts(spark, cfg):
    """So dong moi bang Bronze."""
    return {e: bi.read_bronze(spark, cfg, e).count() for e in bi.ENTITY_ORDER}


def test_append_and_skip_rerun_adds_nothing(spark, fresh_cfg, generated):
    """Nap ngay 1 dung so dong; chay lai bo qua toan bo tep, khong them dong."""
    _, key = generated
    first = bi.ingest_date(spark, fresh_cfg, "2026-10-19")
    assert first["_batch_id"] == "b-0001"
    assert first["skipped_files"] == []
    expected = key["days"]["2026-10-19"]["landing_counts"]
    assert bronze_counts(spark, fresh_cfg) == expected
    assert first["n_input"] == sum(expected.values())

    second = bi.ingest_date(spark, fresh_cfg, "2026-10-19")
    assert second["_batch_id"] is None
    assert second["input_files"] == []
    assert len(second["skipped_files"]) == 6
    assert bronze_counts(spark, fresh_cfg) == expected
    assert run_log.read_ingested_files(spark, fresh_cfg).count() == 6


def test_append_and_skip_batch_ids_increase(spark, fresh_cfg):
    """Moi lan nap tep moi sinh _batch_id moi b-0001, b-0002; metadata dung."""
    bi.ingest_date(spark, fresh_cfg, "2026-10-19")
    r2 = bi.ingest_date(spark, fresh_cfg, "2026-10-20")
    assert r2["_batch_id"] == "b-0002"
    df = bi.read_bronze(spark, fresh_cfg, "orders")
    assert {r["_batch_id"] for r in df.select("_batch_id").distinct().collect()} == {"b-0001", "b-0002"}
    assert df.columns[-3:] == ["_ingest_time", "_source_file", "_batch_id"]
    files = run_log.read_ingested_files(spark, fresh_cfg).where("_batch_id = 'b-0002'").collect()
    assert {f["file_path"] for f in files} >= {"2026-10-20/orders/web_2026-10-20.csv"}


def test_append_and_skip_bad_file_fails_without_writing(spark, generated, tmp_path):
    """Mot tep loi thi lan chay fail, khong ghi gi vao Bronze."""
    import shutil
    out, _ = generated
    landing = tmp_path / "landing"
    shutil.copytree(out / "landing" / "2026-10-19", landing / "2026-10-19")
    (landing / "2026-10-19" / "products_2026-10-19.csv").write_text("", encoding="utf-8")
    cfg = load_config(landing_dir=landing, lakehouse_dir=tmp_path / "lh")
    with pytest.raises(bi.FileCheckError, match="products_2026-10-19.csv"):
        bi.ingest_date(spark, cfg, "2026-10-19")
    assert run_log.read_ingested_files(spark, cfg).count() == 0
    assert not (tmp_path / "lh" / "bronze").exists()
