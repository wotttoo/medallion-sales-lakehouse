"""Test Gold: dim, ngay bi anh huong, fact_sales, KPI, top_customers."""

import datetime as dt
import json

import pytest

import bronze_ingest
import gold_build as gb
import silver_transform as st
from common.config import load_config


@pytest.fixture(scope="module")
def generated(tmp_path_factory):
    """Sinh du lieu vao thu muc tam; tra ve (landing, answer key)."""
    import generate_data
    out = tmp_path_factory.mktemp("gold_data")
    generate_data.main(["--output-dir", str(out)])
    key = json.loads((out / "answer_key" / "answer_key.json").read_text(encoding="utf-8"))
    return out / "landing", key


@pytest.fixture(scope="module")
def day1(spark, generated, tmp_path_factory):
    """Lakehouse da chay Bronze + Silver (v2) cho ngay 1."""
    landing, _ = generated
    cfg = load_config(landing_dir=landing, lakehouse_dir=tmp_path_factory.mktemp("lh_gold"),
                      rules_version="v2")
    bronze_ingest.ingest_date(spark, cfg, "2026-10-19")
    st.process_batch(spark, cfg, "b-0001")
    return cfg


# ---------------------------------------------------------------- T41 dims
def test_dims_written_from_silver(spark, day1, generated):
    """dim_customer, dim_product khop Silver; dim_date phu khoang order_date."""
    _, key = generated
    versions = gb.write_dims(spark, day1)
    assert set(versions) == {"gold.dim_customer", "gold.dim_product", "gold.dim_date"}
    counts = key["days"]["2026-10-19"]["outcomes"]["v2"]["silver_counts"]
    assert gb.read_gold(spark, day1, "dim_customer").count() == counts["customers"]
    product = gb.read_gold(spark, day1, "dim_product")
    assert product.count() == counts["products"] and "category" in product.columns
    dates = gb.read_gold(spark, day1, "dim_date").collect()
    assert [r["date"] for r in dates] == [dt.date(2026, 10, 19)]
    r = dates[0]
    assert (r["day_of_week"], r["month"], r["quarter"], r["year"]) == ("Monday", 10, 4, 2026)
    assert gb.read_gold(spark, day1, "dim_date").columns == \
        ["date", "day_of_week", "month", "quarter", "year"]


def test_dims_overwrite_is_idempotent(spark, day1):
    """Ghi de lan nua khong nhan doi dong."""
    n = gb.read_gold(spark, day1, "dim_customer").count()
    gb.write_dims(spark, day1)
    assert gb.read_gold(spark, day1, "dim_customer").count() == n
