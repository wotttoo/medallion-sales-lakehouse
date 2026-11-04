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
