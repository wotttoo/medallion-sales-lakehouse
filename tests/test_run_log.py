"""Test bang _meta: ingested_files va run_log."""

import pytest

from common import run_log
from common.config import load_config
from common.paths import table_path


@pytest.fixture()
def cfg(tmp_path):
    """Config tro toi lakehouse tam."""
    return load_config(lakehouse_dir=tmp_path / "lakehouse")


def sample_file(name, batch="b-0001", rows=10):
    """Ban ghi ingested_files mau."""
    return {"file_name": name, "file_path": f"landing/2026-10-19/{name}", "entity": "orders",
            "file_date": "2026-10-19", "_batch_id": batch, "row_count": rows}


def test_ingested_empty_when_missing(spark, cfg):
    """Chua co bang thi tra ve rong, dung schema."""
    df = run_log.read_ingested_files(spark, cfg)
    assert df.count() == 0
    assert df.columns == ["file_name", "file_path", "entity", "file_date",
                          "_batch_id", "ingested_at", "row_count"]
    assert run_log.ingested_file_names(spark, cfg) == set()


def test_ingested_record_and_read(spark, cfg):
    """Ghi roi doc lai; ghi them thi cong don (append)."""
    run_log.record_ingested_files(spark, cfg, [sample_file("web_2026-10-19.csv"),
                                               sample_file("app_2026-10-19.csv")])
    run_log.record_ingested_files(spark, cfg, [sample_file("web_2026-10-20.csv", "b-0002", 5)])
    run_log.record_ingested_files(spark, cfg, [])
    assert run_log.ingested_file_names(spark, cfg) == {
        "web_2026-10-19.csv", "app_2026-10-19.csv", "web_2026-10-20.csv"}
    df = run_log.read_ingested_files(spark, cfg)
    assert df.count() == 3
    assert df.where("ingested_at IS NULL").count() == 0
    assert table_path("_meta.ingested_files", cfg).is_dir()
