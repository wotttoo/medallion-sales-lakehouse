"""Doc/ghi bang ho tro trong lakehouse/_meta: ingested_files va run_log."""

from datetime import datetime, timezone

from delta.tables import DeltaTable
from pyspark.sql import types as T

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
