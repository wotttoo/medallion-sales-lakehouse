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
