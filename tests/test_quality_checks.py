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
