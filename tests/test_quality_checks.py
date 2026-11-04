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
