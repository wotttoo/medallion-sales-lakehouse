"""Test bang _meta: ingested_files va run_log."""

import json

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


def test_run_log_write_and_read(spark, cfg):
    """Ghi du cot SPEC 7.6, cot JSON luu dang chuoi."""
    started = run_log.utc_now()
    run_log.write_run_log(spark, cfg, {
        "_batch_id": "b-0001", "processing_date": "2026-10-19", "layer": "silver",
        "rules_version": "v2", "run_started_at": started, "run_finished_at": run_log.utc_now(),
        "status": "success", "input_files": ["web_2026-10-19.csv"], "n_input": 10, "n_valid": 8,
        "n_quarantined": 1, "n_duplicates_removed": 1,
        "n_warn_by_rule": {"phone_not_null": 2}, "delta_versions_written": {"silver.orders": 0},
    })
    df = run_log.read_run_log(spark, cfg)
    assert df.columns == ["run_id", "_batch_id", "processing_date", "layer", "rules_version",
                          "code_version", "run_started_at", "run_finished_at", "status",
                          "input_files", "n_input", "n_valid", "n_quarantined",
                          "n_duplicates_removed", "n_warn_by_rule", "delta_versions_written"]
    row = df.collect()[0]
    assert row["run_id"].startswith("r-")
    assert json.loads(row["n_warn_by_rule"]) == {"phone_not_null": 2}
    assert json.loads(row["input_files"]) == ["web_2026-10-19.csv"]
    assert json.loads(row["delta_versions_written"]) == {"silver.orders": 0}
    assert row["code_version"] == run_log.get_code_version()
    assert run_log.successful_batches(spark, cfg, "silver") == {"b-0001"}
    assert run_log.successful_batches(spark, cfg, "gold") == set()


def test_run_log_code_version_unknown_without_git(tmp_path, monkeypatch):
    """Khong trong repo git, hoac khong co lenh git -> 'unknown', khong loi."""
    assert run_log.get_code_version(tmp_path) == "unknown"
    monkeypatch.setenv("PATH", str(tmp_path))
    assert run_log.get_code_version() == "unknown"


def test_run_log_code_version_in_repo():
    """Trong repo git thi la hash 40 ky tu."""
    v = run_log.get_code_version()
    assert v == "unknown" or len(v) == 40
