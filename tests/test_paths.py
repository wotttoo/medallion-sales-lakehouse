"""Test anh xa ten bang sang duong dan."""

from pathlib import Path

import pytest

from common.config import REPO_ROOT, load_config
from common.paths import canonical_name, table_path, table_uri


def test_bronze_mapping():
    """bronze.orders_raw -> lakehouse/bronze/orders_raw."""
    assert table_path("bronze.orders_raw", lakehouse_root="lakehouse") == \
        Path("lakehouse") / "bronze" / "orders_raw"


def test_quarantine_alias_from_reading():
    """silver.orders_quarantine -> lakehouse/quarantine/orders."""
    assert canonical_name("silver.orders_quarantine") == "quarantine.orders"
    assert table_path("silver.orders_quarantine", lakehouse_root="lakehouse") == \
        Path("lakehouse") / "quarantine" / "orders"
    assert table_path("quarantine.orders", lakehouse_root="lakehouse") == \
        table_path("silver.orders_quarantine", lakehouse_root="lakehouse")


def test_all_layers():
    """Silver, gold va _meta deu anh xa dung thu muc."""
    root = Path("lh")
    assert table_path("silver.orders", lakehouse_root=root) == root / "silver" / "orders"
    assert table_path("gold.fact_sales", lakehouse_root=root) == root / "gold" / "fact_sales"
    assert table_path("_meta.run_log", lakehouse_root=root) == root / "_meta" / "run_log"


def test_uses_config_lakehouse(tmp_path):
    """Lay goc lakehouse tu config (ke ca khi ghi de)."""
    assert table_path("bronze.orders_raw", cfg=load_config()) == \
        REPO_ROOT / "lakehouse" / "bronze" / "orders_raw"
    cfg = load_config(lakehouse_dir=tmp_path)
    assert table_uri("gold.dim_date", cfg=cfg) == str(tmp_path / "gold" / "dim_date")


@pytest.mark.parametrize("bad", ["orders_raw", "unknown.orders", "bronze.", "a.b.c"])
def test_invalid_names(bad):
    """Ten sai thi bao loi."""
    with pytest.raises(ValueError):
        table_path(bad, lakehouse_root="lakehouse")
