"""Test doc config va ghi de duong dan."""

from pathlib import Path

import pytest

from common.config import REPO_ROOT, load_config, resolve_path


def test_default_values_match_spec():
    """Gia tri mac dinh khop SPEC muc 5."""
    cfg = load_config()
    assert cfg["seed"] == 42
    assert cfg["dates"] == ["2026-10-19", "2026-10-20", "2026-10-21"]
    assert cfg["volume"]["orders_per_day"] == 1500
    assert cfg["volume"]["items_per_order"] == [1, 4]
    assert cfg["volume"]["n_customers"] == 200
    assert cfg["volume"]["n_products"] == 50
    assert cfg["channels"] == {"web": "csv", "app": "csv", "partner": "json"}
    assert cfg["rules_version"] == "v2"
    assert cfg["quarantine_rate_threshold"] == 0.10
    assert cfg["volume_drop_threshold"] == 0.5
    assert cfg["enable_schema_evolution_day3"] is True


def test_error_rates_present():
    """Moi ma loi o SPEC 6.3 co ty le trong config."""
    rates = load_config()["error_rates"]
    for code in ["A1_date_ddmmyyyy", "A2_amount_vnd_string", "A3_status_case",
                 "A4_email_case", "B1_dup_cross_channel", "C1_missing_customer_id",
                 "C2_quantity_non_positive", "C3_unknown_product", "W1_missing_phone",
                 "W2_amount_mismatch", "T1_status_update_resend", "T2_late_arriving"]:
        assert 0 < rates[code] < 1, code
    assert rates["B2_dup_same_updated_at_pairs"] == 5


def test_override_paths(tmp_path):
    """Ghi de lakehouse/landing bang thu muc tam."""
    cfg = load_config(lakehouse_dir=tmp_path / "lh", landing_dir=tmp_path / "ld")
    assert resolve_path(cfg, "lakehouse") == tmp_path / "lh"
    assert resolve_path(cfg, "landing") == tmp_path / "ld"


def test_relative_path_resolved_from_repo_root():
    """Duong dan tuong doi tinh tu thu muc goc repo."""
    cfg = load_config()
    assert resolve_path(cfg, "lakehouse") == REPO_ROOT / "lakehouse"
    assert isinstance(resolve_path(cfg, "answer_key"), Path)


def test_override_does_not_mutate_next_load(tmp_path):
    """Ghi de lan truoc khong anh huong lan doc sau."""
    load_config(lakehouse_dir=tmp_path)
    assert load_config()["paths"]["lakehouse"] == "lakehouse"


def test_rules_version_override_and_validation():
    """Ghi de rules_version va bao loi khi gia tri sai."""
    assert load_config(rules_version="v1")["rules_version"] == "v1"
    with pytest.raises(ValueError):
        load_config(rules_version="v3")
