"""Tien ich dung chung cho cac thi nghiem: workspace tam, in bang, dem dong."""

import json
import shutil
import sys
import tempfile
from pathlib import Path

# cho phep chay `python experiments/expN_*.py` tu goc repo
REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

import generate_data  # noqa: E402
from common.config import load_config  # noqa: E402

ENTITIES = ["products", "customers", "orders", "order_items"]


class Workspace:
    """Thu muc tam rieng: landing + answer key sinh moi, lakehouse sach."""

    def __init__(self, name, keep=False):
        """Tao thu muc tam va sinh du lieu vao do."""
        self.root = Path(tempfile.mkdtemp(prefix=f"{name}_"))
        self.keep = keep
        generate_data.main(["--output-dir", str(self.root)])
        self.landing = self.root / "landing"
        self.lakehouse = self.root / "lakehouse"
        self.key = json.loads((self.root / "answer_key" / "answer_key.json").read_text(encoding="utf-8"))

    def cfg(self, rules_version):
        """Config tro toi workspace voi rules_version cho truoc."""
        return load_config(landing_dir=self.landing, lakehouse_dir=self.lakehouse,
                           rules_version=rules_version)

    def cleanup(self):
        """Xoa thu muc tam (tru khi keep=True)."""
        if self.keep:
            print(f"Giu workspace tai: {self.root}")
        else:
            shutil.rmtree(self.root, ignore_errors=True)


def print_table(title, headers, rows):
    """In bang don gian ra console."""
    rows = [[str(c) for c in r] for r in rows]
    widths = [max(len(h), *(len(r[i]) for r in rows)) if rows else len(h)
              for i, h in enumerate(headers)]
    line = "+-" + "-+-".join("-" * w for w in widths) + "-+"
    print(f"\n{title}")
    print(line)
    print("| " + " | ".join(h.ljust(w) for h, w in zip(headers, widths)) + " |")
    print(line)
    for r in rows:
        print("| " + " | ".join(c.ljust(w) for c, w in zip(r, widths)) + " |")
    print(line)


def layer_counts(spark, cfg):
    """So dong moi bang Bronze, Silver, quarantine va Gold."""
    from bronze_ingest import read_bronze
    from gold_build import read_gold
    from silver_transform import read_quarantine, read_silver
    counts = {}
    for e in ENTITIES:
        counts[f"bronze.{e}_raw"] = read_bronze(spark, cfg, e).count()
    for e in ENTITIES:
        counts[f"silver.{e}"] = read_silver(spark, cfg, e).count()
    for e in ENTITIES:
        counts[f"quarantine.{e}"] = read_quarantine(spark, cfg, e).count()
    for t in ["dim_customer", "dim_product", "dim_date", "fact_sales", "daily_revenue_by_category",
              "daily_revenue_by_channel", "top_customers"]:
        counts[f"gold.{t}"] = read_gold(spark, cfg, t).count()
    return counts


def gold_revenue_by_date(spark, cfg):
    """Doanh thu theo order_date tu gold.daily_revenue_by_channel."""
    from gold_build import read_gold
    rows = read_gold(spark, cfg, "daily_revenue_by_channel").groupBy("order_date").sum("revenue").collect()
    return {r["order_date"].isoformat(): int(r[1]) for r in sorted(rows)}


def check(condition, message):
    """Assert co thong bao ro rang (in ra khi dat)."""
    if not condition:
        raise AssertionError(message)
    print(f"  [OK] {message}")
