"""Landing -> Bronze: nap tep nguon nguyen ven (moi cot la string) + metadata.

Quy tac Bronze (SPEC 7.2):
- moi thuc the mot bang bronze.<entity>_raw, ghi append, cho phep mergeSchema;
- kiem tra muc tep (ton tai, khong rong, parse duoc) truoc khi nap;
- bo qua tep da co trong _meta/ingested_files (luy dang muc tep).
"""

import csv
import json
from pathlib import Path

from common.config import resolve_path

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
