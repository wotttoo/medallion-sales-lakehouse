"""Test tong: chay ca pipeline 3 ngay voi v1 va v2 tren lakehouse tam, doi chieu answer key.

Gia tri mong doi lay tu answer_key.json (tinh tu du lieu sach + loi da cai),
khong tinh lai bang pipeline.
"""

import datetime as dt
import json

import pytest

import bronze_ingest
import gold_build as gb
import run_pipeline
import silver_transform as st
from common import run_log
from common.config import load_config
from common.paths import table_path, table_uri

DIMS = {"daily_revenue_by_category": "category", "daily_revenue_by_channel": "channel"}


@pytest.fixture(scope="module")
def generated(tmp_path_factory):
    """Sinh landing + answer key vao thu muc tam."""
    import generate_data
    out = tmp_path_factory.mktemp("e2e_data")
    generate_data.main(["--output-dir", str(out)])
    key = json.loads((out / "answer_key" / "answer_key.json").read_text(encoding="utf-8"))
    return out / "landing", key


def kpi_rows(spark, cfg, table):
    """Bang KPI Gold -> list dict theo dinh dang answer key."""
    dim = DIMS[table]
    return sorted(({"order_date": r["order_date"].isoformat(), dim: r[dim],
                    "revenue": r["revenue"], "n_orders": r["n_orders"]}
                   for r in gb.read_gold(spark, cfg, table).collect()),
                  key=lambda x: (x["order_date"], x[dim]))


def run_all_days(spark, landing, lakehouse, version):
    """Chay pipeline tung ngay; sau moi ngay chup KPI Gold. Tra ve (cfg, snapshot theo ngay)."""
    cfg = load_config(landing_dir=landing, lakehouse_dir=lakehouse, rules_version=version)
    snapshots = {}
    for date in cfg["dates"]:
        run_pipeline.run_date(spark, cfg, date, "all")
        snapshots[date] = {table: kpi_rows(spark, cfg, table) for table in DIMS}
        snapshots[date]["top_customers"] = sorted(
            (r.asDict() for r in gb.read_gold(spark, cfg, "top_customers").collect()),
            key=lambda x: (x["rank"], x["customer_id"]))
    return cfg, snapshots


@pytest.fixture(scope="module", params=["v1", "v2"])
def e2e(request, spark, generated, tmp_path_factory):
    """Ket qua chay day du cho tung rules_version."""
    landing, key = generated
    version = request.param
    cfg, snapshots = run_all_days(spark, landing, tmp_path_factory.mktemp(f"lh_{version}"), version)
    return version, cfg, snapshots, key


def test_revenue_by_category_and_channel_each_day(e2e):
    """Sau moi ngay, KPI theo category va channel khop answer key."""
    version, _, snapshots, key = e2e
    for date, snap in snapshots.items():
        expected = key["days"][date]["outcomes"][version]
        assert snap["daily_revenue_by_category"] == expected["revenue_by_category"], (version, date)
        assert snap["daily_revenue_by_channel"] == expected["revenue_by_channel"], (version, date)


def test_top_customers_final(e2e):
    """top_customers sau ngay cuoi khop answer key."""
    version, cfg, snapshots, key = e2e
    last = cfg["dates"][-1]
    expected = sorted(key["days"][last]["outcomes"][version]["top_customers"],
                      key=lambda x: (x["rank"], x["customer_id"]))
    assert snapshots[last]["top_customers"] == expected


def test_late_arrivals_change_exactly_previous_day(e2e):
    """Don T2 lam doi doanh thu cua ngay cu; cac ngay khac khong bi dong vao."""
    version, cfg, snapshots, key = e2e
    dates = cfg["dates"]
    for i in range(1, len(dates)):
        before = {r["order_date"]: r for r in snapshots[dates[i - 1]]["daily_revenue_by_channel"]}
        after = {r["order_date"]: r for r in snapshots[dates[i]]["daily_revenue_by_channel"]}
        total = lambda snap, d: sum(r["revenue"] for r in snap["daily_revenue_by_channel"]
                                    if r["order_date"] == d)
        prev_day = dates[i - 1]
        assert total(snapshots[dates[i]], prev_day) > total(snapshots[dates[i - 1]], prev_day)
        assert total(snapshots[dates[i]], prev_day) == \
            key["days"][dates[i]]["outcomes"][version]["revenue_by_date"][prev_day]
        for d in dates[: i - 1]:          # ngay xa hon khong doi
            assert total(snapshots[dates[i]], d) == total(snapshots[dates[i - 1]], d)
        assert dates[i] not in before and dates[i] in after


def test_silver_and_quarantine_counts(spark, e2e):
    """So dong Silver va quarantine sau 3 ngay khop answer key; run log ghi dung tung lo."""
    version, cfg, _, key = e2e
    last = cfg["dates"][-1]
    for entity, n in key["days"][last]["outcomes"][version]["silver_counts"].items():
        assert st.read_silver(spark, cfg, entity).count() == n
        n_q = sum(key["days"][d]["outcomes"][version]["entities"][entity]["n_quarantined"]
                  for d in cfg["dates"])
        assert st.read_quarantine(spark, cfg, entity).count() == n_q
    logs = run_log.read_run_log(spark, cfg).where("layer = 'silver'").orderBy("_batch_id").collect()
    assert [r["status"] for r in logs] == ["success"] * 3
    for r, d in zip(logs, cfg["dates"]):
        ents = key["days"][d]["outcomes"][version]["entities"]
        assert r["n_input"] == sum(e["n_input"] for e in ents.values())
        assert r["n_quarantined"] == sum(e["n_quarantined"] for e in ents.values())
        assert json.loads(r["n_warn_by_rule"]) == key["days"][d]["outcomes"][version]["n_warn_by_rule"]
        assert r["rules_version"] == version


def test_reading_values_spec_section_9(spark, e2e):
    """Gia tri reading (SPEC muc 9): ba dong Bronze, ORD-1001 Silver, ORD-1002 quarantine."""
    version, cfg, _, _ = e2e
    rows = (bronze_ingest.read_bronze(spark, cfg, "orders")
            .where("order_id IN ('ORD-1001', 'ORD-1002')").orderBy("_ingest_time").collect())
    assert [(r["order_id"], r["_source_file"]) for r in rows] == [
        ("ORD-1001", "web_2026-10-19.csv"), ("ORD-1001", "app_2026-10-19.csv"),
        ("ORD-1002", "partner_2026-10-19.json")]
    assert len({r["_batch_id"] for r in rows}) == 1
    assert rows[1]["_ingest_time"] > rows[0]["_ingest_time"]

    silver = st.read_silver(spark, cfg, "orders").where("order_id = 'ORD-1001'").collect()
    assert len(silver) == 1
    assert dict(st.read_silver(spark, cfg, "orders").dtypes)["order_date"] == "date"
    s = silver[0]
    assert (s["order_date"], s["total_amount"], s["status"]) == (dt.date(2026, 10, 19), 150000, "delivered")

    q = (spark.read.format("delta").load(table_uri("silver.orders_quarantine", cfg))
         .where("order_id = 'ORD-1002'").collect())
    assert len(q) == 1
    expected_rules = {"v1": "customer_id_not_null; order_date_valid_format; total_amount_numeric",
                      "v2": "customer_id_not_null"}[version]
    assert q[0]["_failed_rules"] == expected_rules
    assert q[0]["order_date"] == "19/10/2026" and q[0]["total_amount"] == "150.000đ"


def test_table_names_exist(spark, e2e):
    """Moi bang trong SPEC 7 ton tai dung ten logic."""
    from delta.tables import DeltaTable
    _, cfg, _, _ = e2e
    names = ([f"bronze.{e}_raw" for e in st.ENTITY_ORDER] + [f"silver.{e}" for e in st.ENTITY_ORDER]
             + [f"quarantine.{e}" for e in st.ENTITY_ORDER]
             + [f"gold.{t}" for t in ["dim_customer", "dim_product", "dim_date", "fact_sales",
                                      "daily_revenue_by_category", "daily_revenue_by_channel",
                                      "top_customers"]]
             + ["_meta.ingested_files", "_meta.run_log"])
    for name in names:
        assert DeltaTable.isDeltaTable(spark, str(table_path(name, cfg))), name
