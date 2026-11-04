"""Doc/ghi bang ho tro trong lakehouse/_meta: ingested_files va run_log."""

import json
import subprocess
import uuid
from datetime import datetime, timezone

from delta.tables import DeltaTable
from pyspark.sql import types as T

from common.config import REPO_ROOT
from common.paths import table_uri

INGESTED_FILES = "_meta.ingested_files"

INGESTED_FILES_SCHEMA = T.StructType([
    T.StructField("file_name", T.StringType(), False),
    T.StructField("file_path", T.StringType(), False),
    T.StructField("entity", T.StringType(), False),
    T.StructField("file_date", T.StringType(), False),
    T.StructField("_batch_id", T.StringType(), False),
    T.StructField("ingested_at", T.TimestampType(), False),
    T.StructField("row_count", T.LongType(), False),
])


def utc_now():
    """Gio he thong (UTC) - chi dung cho cot metadata."""
    return datetime.now(timezone.utc)


def table_exists(spark, path):
    """Kiem tra duong dan da la bang Delta chua."""
    return DeltaTable.isDeltaTable(spark, str(path))


def read_ingested_files(spark, cfg):
    """Doc bang ingested_files (DataFrame rong neu chua co)."""
    path = table_uri(INGESTED_FILES, cfg)
    if not table_exists(spark, path):
        return spark.createDataFrame([], INGESTED_FILES_SCHEMA)
    return spark.read.format("delta").load(path)


def ingested_file_names(spark, cfg):
    """Tap ten tep da nap vao Bronze (de bo qua khi chay lai)."""
    return {r["file_name"] for r in read_ingested_files(spark, cfg).select("file_name").collect()}


def record_ingested_files(spark, cfg, records):
    """Them cac tep vua nap vao ingested_files.

    records: list dict co du cot cua INGESTED_FILES_SCHEMA (ingested_at co the bo trong).
    """
    if not records:
        return
    now = utc_now()
    rows = [(r["file_name"], r["file_path"], r["entity"], r["file_date"], r["_batch_id"],
             r.get("ingested_at") or now, int(r["row_count"])) for r in records]
    df = spark.createDataFrame(rows, INGESTED_FILES_SCHEMA)
    df.write.format("delta").mode("append").save(table_uri(INGESTED_FILES, cfg))


# ---------------------------------------------------------------------------
# run_log: mot dong moi lan chay mot layer (SPEC 7.6)
# ---------------------------------------------------------------------------
RUN_LOG = "_meta.run_log"

RUN_LOG_SCHEMA = T.StructType([
    T.StructField("run_id", T.StringType(), False),
    T.StructField("_batch_id", T.StringType(), True),
    T.StructField("processing_date", T.StringType(), True),
    T.StructField("layer", T.StringType(), False),
    T.StructField("rules_version", T.StringType(), True),
    T.StructField("code_version", T.StringType(), False),
    T.StructField("run_started_at", T.TimestampType(), False),
    T.StructField("run_finished_at", T.TimestampType(), True),
    T.StructField("status", T.StringType(), False),
    T.StructField("input_files", T.StringType(), True),           # chuoi JSON
    T.StructField("n_input", T.LongType(), True),
    T.StructField("n_valid", T.LongType(), True),
    T.StructField("n_quarantined", T.LongType(), True),
    T.StructField("n_duplicates_removed", T.LongType(), True),
    T.StructField("n_warn_by_rule", T.StringType(), True),        # chuoi JSON
    T.StructField("delta_versions_written", T.StringType(), True),  # chuoi JSON
])
JSON_COLUMNS = ("input_files", "n_warn_by_rule", "delta_versions_written")


def get_code_version(repo_dir=None):
    """Commit hash hien tai (git rev-parse HEAD); 'unknown' neu khong co git/repo."""
    try:
        out = subprocess.run(["git", "rev-parse", "HEAD"], cwd=str(repo_dir or REPO_ROOT),
                             capture_output=True, text=True, timeout=10)
    except (OSError, subprocess.SubprocessError):
        return "unknown"
    if out.returncode != 0 or not out.stdout.strip():
        return "unknown"
    return out.stdout.strip()


def new_run_id():
    """Ma lan chay duy nhat (khong dung cho logic nghiep vu)."""
    return "r-" + uuid.uuid4().hex[:12]


def to_json(value):
    """Doi gia tri Python sang chuoi JSON on dinh (None giu nguyen)."""
    if value is None or isinstance(value, str):
        return value
    return json.dumps(value, sort_keys=True, ensure_ascii=False)


def write_run_log(spark, cfg, record):
    """Them mot dong vao run_log. Cot JSON co the truyen dict/list."""
    row = {f.name: record.get(f.name) for f in RUN_LOG_SCHEMA.fields}
    for col in JSON_COLUMNS:
        row[col] = to_json(row[col])
    for col in ("n_input", "n_valid", "n_quarantined", "n_duplicates_removed"):
        if row[col] is not None:
            row[col] = int(row[col])
    row["code_version"] = row["code_version"] or get_code_version()
    row["run_id"] = row["run_id"] or new_run_id()
    df = spark.createDataFrame([tuple(row[f.name] for f in RUN_LOG_SCHEMA.fields)], RUN_LOG_SCHEMA)
    df.write.format("delta").mode("append").save(table_uri(RUN_LOG, cfg))
    return row


def read_run_log(spark, cfg):
    """Doc run_log (rong neu chua co)."""
    path = table_uri(RUN_LOG, cfg)
    if not table_exists(spark, path):
        return spark.createDataFrame([], RUN_LOG_SCHEMA)
    return spark.read.format("delta").load(path)


def successful_batches(spark, cfg, layer):
    """Tap _batch_id da chay thanh cong o mot layer."""
    df = read_run_log(spark, cfg).where(f"layer = '{layer}' AND status = 'success'")
    return {r["_batch_id"] for r in df.select("_batch_id").distinct().collect() if r["_batch_id"]}
