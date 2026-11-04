"""Landing -> Bronze: nap tep nguon nguyen ven (moi cot la string) + metadata.

Quy tac Bronze (SPEC 7.2):
- moi thuc the mot bang bronze.<entity>_raw, ghi append, cho phep mergeSchema;
- kiem tra muc tep (ton tai, khong rong, parse duoc) truoc khi nap;
- bo qua tep da co trong _meta/ingested_files (luy dang muc tep).
"""

import csv
import json
from datetime import timedelta
from pathlib import Path

from pyspark.sql import functions as F

from common.config import resolve_path
from common.run_log import utc_now

# thu tu nap cac thuc the trong mot lo
ENTITY_ORDER = ["products", "customers", "orders", "order_items"]

# cot bat buoc cua tep nguon (SPEC 6.1)
SOURCE_COLUMNS = {
    "orders": ["order_id", "customer_id", "order_date", "total_amount", "status",
               "channel", "updated_at"],
    "order_items": ["order_id", "product_id", "quantity", "unit_price"],
    "customers": ["customer_id", "name", "email", "phone", "city"],
    "products": ["product_id", "name", "category", "list_price"],
}


class FileCheckError(Exception):
    """Tep nguon khong dat kiem tra muc tep (lan chay phai fail)."""


# ---------------------------------------------------------------------------
# Liet ke tep cua mot ngay
# ---------------------------------------------------------------------------
def list_landing_files(cfg, date, landing_dir=None):
    """Tra ve danh sach tep cua mot ngay theo thu tu co dinh.

    Thu tu: products, customers, orders (web -> app -> partner, theo config), order_items.
    Moi phan tu: dict entity, file_name, path, fmt, file_date.
    """
    root = Path(landing_dir) if landing_dir else resolve_path(cfg, "landing")
    day_dir = root / date
    files = []
    for entity in ENTITY_ORDER:
        if entity == "orders":
            for channel, fmt in cfg["channels"].items():
                name = f"{channel}_{date}.{fmt}"
                files.append({"entity": entity, "file_name": name, "fmt": fmt,
                              "path": day_dir / "orders" / name, "file_date": date})
        else:
            name = f"{entity}_{date}.csv"
            files.append({"entity": entity, "file_name": name, "fmt": "csv",
                          "path": day_dir / name, "file_date": date})
    return files


# ---------------------------------------------------------------------------
# Kiem tra muc tep
# ---------------------------------------------------------------------------
def count_csv_rows(path, required_columns):
    """Dem dong du lieu CSV; bao loi neu thieu cot hoac dong sai so cot."""
    with Path(path).open("r", encoding="utf-8", newline="") as f:
        reader = csv.reader(f)
        header = next(reader, None)
        if header is None:
            raise FileCheckError(f"Tep rong: {Path(path).name}")
        missing = [c for c in required_columns if c not in header]
        if missing:
            raise FileCheckError(f"Tep {Path(path).name} thieu cot {missing}")
        n = 0
        for line_no, row in enumerate(reader, start=2):
            if not row:
                continue
            if len(row) != len(header):
                raise FileCheckError(f"Tep {Path(path).name} dong {line_no}: "
                                     f"{len(row)} cot, mong doi {len(header)}")
            n += 1
    return n


def count_json_rows(path, required_columns):
    """Dem dong JSON lines; bao loi neu dong khong parse duoc hoac thieu cot."""
    n = 0
    with Path(path).open("r", encoding="utf-8") as f:
        for line_no, line in enumerate(f, start=1):
            if not line.strip():
                continue
            try:
                rec = json.loads(line)
            except json.JSONDecodeError as exc:
                raise FileCheckError(f"Tep {Path(path).name} dong {line_no}: JSON loi ({exc})")
            if not isinstance(rec, dict):
                raise FileCheckError(f"Tep {Path(path).name} dong {line_no}: khong phai object")
            missing = [c for c in required_columns if c not in rec]
            if missing:
                raise FileCheckError(f"Tep {Path(path).name} dong {line_no} thieu cot {missing}")
            n += 1
    return n


def check_file(file_info):
    """Kiem tra tep ton tai, khong rong, parse duoc. Tra ve so dong du lieu."""
    path = Path(file_info["path"])
    name = file_info["file_name"]
    if not path.is_file():
        raise FileCheckError(f"Khong tim thay tep: {name} ({path})")
    if path.stat().st_size == 0:
        raise FileCheckError(f"Tep rong: {name}")
    required = SOURCE_COLUMNS[file_info["entity"]]
    try:
        if file_info["fmt"] == "json":
            n = count_json_rows(path, required)
        else:
            n = count_csv_rows(path, required)
    except UnicodeDecodeError as exc:
        raise FileCheckError(f"Tep {name} khong doc duoc dang UTF-8 ({exc})")
    if n == 0:
        raise FileCheckError(f"Tep khong co dong du lieu: {name}")
    return n


# ---------------------------------------------------------------------------
# Doc tep thanh DataFrame (moi cot la string) + metadata
# ---------------------------------------------------------------------------
def next_ingest_time(previous=None):
    """Gio nap cua tep tiep theo: gio he thong, luon tang ngat so voi tep truoc.

    Nho vay dong cua tep doc sau (vd app) luon co _ingest_time lon hon (vd web).
    """
    now = utc_now()
    if previous is not None and now <= previous:
        now = previous + timedelta(milliseconds=1)
    return now


def read_file_as_strings(spark, file_info, ingest_time):
    """Doc mot tep nguon, moi cot la string, them _ingest_time va _source_file.

    Cot nguon xep dung thu tu SPEC; cot moi (vd promo_code) dat sau, theo ten.
    """
    path = str(file_info["path"])
    if file_info["fmt"] == "json":
        df = spark.read.option("primitivesAsString", True).option("mode", "FAILFAST").json(path)
    else:
        df = (spark.read.option("header", True).option("inferSchema", False)
              .option("encoding", "UTF-8").option("mode", "FAILFAST").csv(path))
    base = SOURCE_COLUMNS[file_info["entity"]]
    extra = sorted(c for c in df.columns if c not in base)
    df = df.select([F.col(c).cast("string").alias(c) for c in base + extra])
    return (df.withColumn("_ingest_time", F.lit(ingest_time).cast("timestamp"))
              .withColumn("_source_file", F.lit(file_info["file_name"])))
