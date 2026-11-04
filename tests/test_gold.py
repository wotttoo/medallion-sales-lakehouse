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


# ---------------------------------------------------------------- T42 ngay bi anh huong
@pytest.fixture(scope="module")
def two_days(spark, generated, tmp_path_factory):
    """Lakehouse da chay Bronze + Silver (v2) cho ngay 1 va ngay 2."""
    landing, _ = generated
    cfg = load_config(landing_dir=landing, lakehouse_dir=tmp_path_factory.mktemp("lh_gold2"),
                      rules_version="v2")
    for d in ["2026-10-19", "2026-10-20"]:
        bronze_ingest.ingest_date(spark, cfg, d)
    for b in st.all_batches(spark, cfg):
        st.process_batch(spark, cfg, b)
    return cfg


def test_affected_dates_first_batch(spark, day1):
    """Lo ngay 1 chi anh huong ngay 1."""
    assert gb.affected_dates(spark, day1, "b-0001") == [dt.date(2026, 10, 19)]


def test_affected_dates_include_late_and_updates(spark, two_days):
    """Lo ngay 2 anh huong ca ngay 1 (don den muon T2 va don cap nhat T1)."""
    assert gb.affected_dates(spark, two_days, "b-0002") == [dt.date(2026, 10, 19),
                                                           dt.date(2026, 10, 20)]
    assert gb.all_order_dates(spark, two_days) == [dt.date(2026, 10, 19), dt.date(2026, 10, 20)]


# ---------------------------------------------------------------- T43 fact_sales
def test_fact_sales_grain_and_columns(spark, day1):
    """Mot dong moi san pham trong don; dung cot SPEC; phan vung theo order_date."""
    gb.write_fact_sales(spark, day1, [dt.date(2026, 10, 19)])
    fact = gb.read_gold(spark, day1, "fact_sales")
    assert fact.columns == gb.FACT_COLUMNS
    assert fact.count() == st.read_silver(spark, day1, "order_items").count()
    assert fact.select("order_id", "product_id").distinct().count() == fact.count()
    from delta.tables import DeltaTable
    detail = DeltaTable.forPath(spark, gb.gold_path(day1, "fact_sales")).detail().collect()[0]
    assert detail["partitionColumns"] == ["order_date"]


def test_fact_sales_replace_is_idempotent(spark, day1):
    """Ghi lai cung ngay khong nhan doi dong."""
    n = gb.read_gold(spark, day1, "fact_sales").count()
    gb.write_fact_sales(spark, day1, [dt.date(2026, 10, 19)])
    assert gb.read_gold(spark, day1, "fact_sales").count() == n


def test_fact_sales_old_date_rewritten(spark, two_days):
    """Lo ngay 2 ghi de ca ngay 1: dong den muon va trang thai T1 moi xuat hien."""
    d1, d2 = dt.date(2026, 10, 19), dt.date(2026, 10, 20)
    gb.write_fact_sales(spark, two_days, gb.affected_dates(spark, two_days, "b-0001"))
    gb.write_fact_sales(spark, two_days, gb.affected_dates(spark, two_days, "b-0002"))
    fact = gb.read_gold(spark, two_days, "fact_sales")
    assert fact.count() == st.read_silver(spark, two_days, "order_items").count()
    silver_status = {r["order_id"]: r["status"] for r in
                     st.read_silver(spark, two_days, "orders").where(f"order_date = '{d1}'").collect()}
    fact_status = {r["order_id"]: r["status"] for r in
                   fact.where(f"order_date = '{d1}'").select("order_id", "status").distinct().collect()}
    assert all(silver_status[o] == s for o, s in fact_status.items())
    assert fact.where(f"order_date = '{d2}'").count() > 0


# ---------------------------------------------------------------- T44 KPI theo category
def kpi_rows(spark, cfg, table, dim):
    """Doc bang KPI thanh list dict giong dinh dang answer key."""
    return sorted(({"order_date": r["order_date"].isoformat(), dim: r[dim],
                    "revenue": r["revenue"], "n_orders": r["n_orders"]}
                   for r in gb.read_gold(spark, cfg, table).collect()),
                  key=lambda x: (x["order_date"], x[dim]))


def build_until(spark, cfg, batches):
    """Dung Gold (dims, fact, KPI co san) cho lan luot cac lo."""
    for b in batches:
        dates = gb.affected_dates(spark, cfg, b)
        gb.write_dims(spark, cfg)
        gb.write_fact_sales(spark, cfg, dates)
        gb.write_revenue_by_category(spark, cfg, dates)
        if hasattr(gb, "write_revenue_by_channel"):
            gb.write_revenue_by_channel(spark, cfg, dates)


def test_revenue_by_category_day1(spark, day1, generated):
    """Doanh thu order_date x category sau ngay 1 khop answer key (v2)."""
    _, key = generated
    build_until(spark, day1, ["b-0001"])
    assert kpi_rows(spark, day1, "daily_revenue_by_category", "category") == \
        key["days"]["2026-10-19"]["outcomes"]["v2"]["revenue_by_category"]


def test_revenue_by_category_after_late_arrivals(spark, two_days, generated):
    """Sau ngay 2, doanh thu ngay 1 thay doi do don den muon va van khop answer key."""
    _, key = generated
    build_until(spark, two_days, ["b-0001", "b-0002"])
    got = kpi_rows(spark, two_days, "daily_revenue_by_category", "category")
    assert got == key["days"]["2026-10-20"]["outcomes"]["v2"]["revenue_by_category"]
    assert got != key["days"]["2026-10-19"]["outcomes"]["v2"]["revenue_by_category"]
