"""Test kiem tra muc tap du lieu."""

import pytest

import quality_checks as qc


def stats(n_input, n_valid, n_quarantined, n_dup):
    """Thong ke mot thuc the."""
    return {"n_input": n_input, "n_valid": n_valid, "n_quarantined": n_quarantined,
            "n_duplicates_removed": n_dup}


# ---------------------------------------------------------------- T48 conservation
def test_conservation_ok():
    """Dang thuc dung thi khong raise."""
    assert qc.check_record_conservation({"orders": stats(10, 7, 2, 1),
                                         "products": stats(0, 0, 0, 0)}) == \
        {"record_conservation": "ok"}


def test_conservation_violation_raises():
    """Lech mot ban ghi thi raise, thong bao neu ten thuc the."""
    with pytest.raises(qc.QualityCheckError, match="order_items"):
        qc.check_record_conservation({"orders": stats(10, 7, 2, 1),
                                      "order_items": stats(10, 7, 2, 0)})


# ---------------------------------------------------------------- T49 quarantine_rate
def test_quarantine_rate_below_threshold_ok():
    """Duoi nguong thi tra ve ty le tung thuc the; thuc the rong bi bo qua."""
    out = qc.check_quarantine_rate({"orders": stats(100, 90, 9, 1),
                                    "products": stats(0, 0, 0, 0)}, 0.10)
    assert out == {"quarantine_rate": {"orders": 0.09}}


@pytest.mark.parametrize("n_quarantined", [10, 30])
def test_quarantine_rate_at_or_above_threshold_fails(n_quarantined):
    """Cham (=) hoac vuot nguong thi raise."""
    with pytest.raises(qc.QualityCheckError, match="orders"):
        qc.check_quarantine_rate({"orders": stats(100, 100 - n_quarantined, n_quarantined, 0)}, 0.10)


# ---------------------------------------------------------------- T50 volume_check
def test_volume_skipped_on_first_day():
    """Ngay dau khong co du lieu truoc thi bo qua."""
    assert qc.check_volume_counts("2026-10-19", {"2026-10-19": 1535}, 0.5) == \
        {"volume_check": "skipped (ngay dau)"}


def test_volume_ok_and_drop_fails():
    """Du so don thi qua; tut xuong duoi 50% trung binh thi fail."""
    counts = {"2026-10-19": 1500, "2026-10-20": 1700, "2026-10-21": 800}
    out = qc.check_volume_counts("2026-10-20", counts, 0.5)
    assert out["volume_check"]["previous_average"] == 1500.0
    assert qc.check_volume_counts("2026-10-21", dict(counts, **{"2026-10-21": 800}), 0.5)
    with pytest.raises(qc.QualityCheckError, match="2026-10-21"):
        qc.check_volume_counts("2026-10-21", dict(counts, **{"2026-10-21": 700}), 0.5)


def test_volume_reads_ingested_files(spark, tmp_path):
    """Dem so dong orders theo ngay tu ingested_files (cong cac kenh)."""
    from common import run_log
    from common.config import load_config
    cfg = load_config(lakehouse_dir=tmp_path / "lh")
    recs = [{"file_name": f"{ch}_{d}.csv", "file_path": "x", "entity": "orders", "file_date": d,
             "_batch_id": b, "row_count": n}
            for d, b, n in [("2026-10-19", "b-0001", 100), ("2026-10-20", "b-0002", 30)]
            for ch in ["web", "app"]]
    recs.append({"file_name": "products_2026-10-20.csv", "file_path": "x", "entity": "products",
                 "file_date": "2026-10-20", "_batch_id": "b-0002", "row_count": 999})
    run_log.record_ingested_files(spark, cfg, recs)
    assert qc.daily_order_counts(spark, cfg) == {"2026-10-19": 200, "2026-10-20": 60}
    with pytest.raises(qc.QualityCheckError):
        qc.check_volume(spark, cfg, "2026-10-20")


# ---------------------------------------------------------------- T51 reconciliation
@pytest.fixture()
def small_lakehouse(spark, tmp_path):
    """Silver nho viet tay + Gold dung tu do (2 ngay, co don huy)."""
    import datetime as dt
    import gold_build as gb
    import silver_transform as st
    from common.config import load_config
    from common.paths import table_uri
    cfg = load_config(lakehouse_dir=tmp_path / "lh")
    t = dt.datetime(2026, 10, 19, 12)
    meta = (t, "f.csv", "b-0001")
    d1, d2 = dt.date(2026, 10, 19), dt.date(2026, 10, 20)
    tables = {
        "products": [("P-1", "Sach", "Books", 10), ("P-2", "Son", "Beauty", 20)],
        "customers": [("C-1", "A", "a@x.com", None, "Hue"), ("C-2", "B", "b@x.com", "09", "Hue")],
        "orders": [("O1", "C-1", d1, 30, "delivered", "web", t, None),
                   ("O2", "C-2", d1, 20, "cancelled", "app", t, None),
                   ("O3", "C-2", d2, 10, "pending", "partner", t, "SALE10")],
        "order_items": [("O1", "P-1", 1, 10, 10), ("O1", "P-2", 1, 20, 20),
                        ("O2", "P-2", 1, 20, 20), ("O3", "P-1", 1, 10, 10)],
    }
    types = {"products": "string, string, string, bigint",
             "customers": "string, string, string, string, string",
             "orders": "string, string, date, bigint, string, string, timestamp, string",
             "order_items": "string, string, int, bigint, bigint"}
    for entity, rows in tables.items():
        cols = st.silver_columns(entity)
        schema = ", ".join(f"{c} {ty}" for c, ty in zip(
            cols, types[entity].split(", ") + ["timestamp", "string", "string"]))
        spark.createDataFrame([r + meta for r in rows], schema) \
            .write.format("delta").save(table_uri(f"silver.{entity}", cfg))
    gb.build_gold(spark, cfg, dates=gb.all_order_dates(spark, cfg))
    return cfg


def test_reconciliation_ok(spark, small_lakehouse):
    """Gold vua dung tu Silver thi khop; don huy khong tinh."""
    out = qc.check_revenue_reconciliation(spark, small_lakehouse)
    assert out == {"revenue_reconciliation": {"2026-10-19": 30, "2026-10-20": 10}}


def test_reconciliation_detects_mismatch(spark, small_lakehouse):
    """Sua tay bang KPI cho lech -> fail, neu ro bang va ngay."""
    import gold_build as gb
    path = gb.gold_path(small_lakehouse, "daily_revenue_by_channel")
    bad = gb.read_gold(spark, small_lakehouse, "daily_revenue_by_channel") \
        .withColumn("revenue", gb.F.col("revenue") + 1)
    bad.write.format("delta").mode("overwrite").save(path)
    with pytest.raises(qc.QualityCheckError, match="daily_revenue_by_channel.*2026-10-19"):
        qc.check_revenue_reconciliation(spark, small_lakehouse)


# ---------------------------------------------------------------- T52 no_orphan_keys
def test_orphan_keys_ok(spark, small_lakehouse):
    """Gold dung tu Silver nhat quan thi khong co khoa mo coi."""
    assert qc.check_no_orphan_keys(spark, small_lakehouse) == {"no_orphan_keys": "ok"}


def test_orphan_keys_detected(spark, small_lakehouse):
    """Xoa mot khach khoi dim_customer -> fail, neu ro cot va vi du."""
    import gold_build as gb
    path = gb.gold_path(small_lakehouse, "dim_customer")
    gb.read_gold(spark, small_lakehouse, "dim_customer").where("customer_id <> 'C-2'") \
        .write.format("delta").mode("overwrite").save(path)
    with pytest.raises(qc.QualityCheckError, match="customer_id -> dim_customer.*C-2"):
        qc.check_no_orphan_keys(spark, small_lakehouse)


# ---------------------------------------------------------------- T53 pipeline fail
@pytest.fixture(scope="module")
def landing_dir(tmp_path_factory):
    """Sinh landing vao thu muc tam."""
    import generate_data
    out = tmp_path_factory.mktemp("qc_data")
    generate_data.main(["--output-dir", str(out)])
    return out / "landing"


def test_pipeline_fails_on_quarantine_rate(spark, landing_dir, tmp_path):
    """Nguong cach ly qua thap -> exit code 1, run log silver status = failed."""
    import yaml
    import run_pipeline
    from common import run_log
    from common.config import DEFAULT_CONFIG_PATH, load_config
    cfg_data = yaml.safe_load(DEFAULT_CONFIG_PATH.read_text(encoding="utf-8"))
    cfg_data["quarantine_rate_threshold"] = 0.001
    cfg_path = tmp_path / "config.yaml"
    cfg_path.write_text(yaml.safe_dump(cfg_data), encoding="utf-8")
    lh = tmp_path / "lh"
    code = run_pipeline.main(["--processing-date", "2026-10-19", "--layer", "all",
                              "--config", str(cfg_path), "--lakehouse-dir", str(lh),
                              "--landing-dir", str(landing_dir)], spark=spark)
    assert code == 1
    cfg = load_config(cfg_path, lakehouse_dir=lh)
    logs = {(r["layer"], r["status"]) for r in run_log.read_run_log(spark, cfg).collect()}
    assert logs == {("bronze", "success"), ("silver", "failed")}
    # gold khong chay sau khi silver fail
    assert not (lh / "gold").exists()


def test_pipeline_fails_without_landing(spark, tmp_path):
    """Thieu tep landing -> bronze failed, exit code khac 0."""
    import run_pipeline
    from common import run_log
    from common.config import load_config
    code = run_pipeline.main(["--processing-date", "2026-10-19", "--layer", "bronze",
                              "--lakehouse-dir", str(tmp_path / "lh"),
                              "--landing-dir", str(tmp_path / "empty")], spark=spark)
    assert code == 1
    rows = run_log.read_run_log(spark, load_config(lakehouse_dir=tmp_path / "lh")).collect()
    assert [(r["layer"], r["status"]) for r in rows] == [("bronze", "failed")]
