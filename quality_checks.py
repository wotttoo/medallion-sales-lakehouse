"""Kiem tra muc tap du lieu (SPEC 7.5). Moi check vi pham thi raise QualityCheckError.

Check sau layer:
- bronze: volume_check
- silver: record_conservation, quarantine_rate
- gold:   revenue_reconciliation, no_orphan_keys
"""

from pyspark.sql import functions as F

from common.run_log import read_ingested_files
from gold_build import read_gold
from silver_transform import read_silver


class QualityCheckError(Exception):
    """Mot check muc tap du lieu that bai (pipeline phai fail)."""


# ---------------------------------------------------------------------------
# record_conservation
# ---------------------------------------------------------------------------
def check_record_conservation(entity_stats):
    """n_input = n_valid + n_quarantined + n_duplicates_removed cho moi thuc the cua lo.

    entity_stats: dict entity -> dict cac so dem (tu silver_transform.process_batch).
    """
    # BEGIN SOLUTION
    broken = {}
    for entity, s in entity_stats.items():
        total = s["n_valid"] + s["n_quarantined"] + s["n_duplicates_removed"]
        if s["n_input"] != total:
            broken[entity] = f"n_input={s['n_input']} != {total}"
    # END SOLUTION
    if broken:
        raise QualityCheckError(f"record_conservation that bai: {broken}")
    return {"record_conservation": "ok"}


# ---------------------------------------------------------------------------
# quarantine_rate
# ---------------------------------------------------------------------------
def check_quarantine_rate(entity_stats, threshold):
    """Ty le cach ly cua lo (moi thuc the) phai < threshold; cham nguong thi fail."""
    # BEGIN SOLUTION
    rates, broken = {}, {}
    for entity, s in entity_stats.items():
        if s["n_input"] == 0:
            continue
        rate = s["n_quarantined"] / s["n_input"]
        rates[entity] = round(rate, 4)
        if rate >= threshold:
            broken[entity] = round(rate, 4)
    # END SOLUTION
    if broken:
        raise QualityCheckError(f"quarantine_rate >= {threshold}: {broken}")
    return {"quarantine_rate": rates}


# ---------------------------------------------------------------------------
# volume_check
# ---------------------------------------------------------------------------
def daily_order_counts(spark, cfg):
    """So dong orders da nap theo ngay tep (tu _meta/ingested_files)."""
    rows = (read_ingested_files(spark, cfg).where("entity = 'orders'")
            .groupBy("file_date").sum("row_count").collect())
    return {r["file_date"]: int(r[1]) for r in rows}


def check_volume_counts(processing_date, counts, threshold):
    """So don ngay xu ly >= threshold x trung binh cac ngay truoc; bo qua ngay dau."""
    previous = [n for d, n in counts.items() if d < processing_date]
    if not previous:
        return {"volume_check": "skipped (ngay dau)"}
    current = counts.get(processing_date, 0)
    average = sum(previous) / len(previous)
    if current < threshold * average:
        raise QualityCheckError(
            f"volume_check that bai: {processing_date} co {current} don < "
            f"{threshold} x trung binh {average:.1f}")
    return {"volume_check": {"current": current, "previous_average": round(average, 1)}}


def check_volume(spark, cfg, processing_date):
    """volume_check cho ngay xu ly, doc so dong tu ingested_files."""
    return check_volume_counts(processing_date, daily_order_counts(spark, cfg),
                               cfg["volume_drop_threshold"])


# ---------------------------------------------------------------------------
# revenue_reconciliation
# ---------------------------------------------------------------------------
def silver_revenue_by_date(spark, cfg):
    """Doanh thu theo order_date tinh truc tiep tu Silver (khong qua Gold)."""
    orders = read_silver(spark, cfg, "orders").where(F.col("status") != "cancelled")
    rows = (read_silver(spark, cfg, "order_items").join(orders.select("order_id", "order_date"), "order_id")
            .groupBy("order_date").agg(F.sum("line_amount").alias("revenue")).collect())
    return {r["order_date"]: int(r["revenue"]) for r in rows}


def gold_revenue_by_date(spark, cfg, table):
    """Tong revenue theo order_date cua mot bang KPI Gold."""
    rows = (read_gold(spark, cfg, table).groupBy("order_date")
            .agg(F.sum("revenue").alias("revenue")).collect())
    return {r["order_date"]: int(r["revenue"]) for r in rows}


def check_revenue_reconciliation(spark, cfg):
    """Doanh thu Gold theo ngay (ca hai bang KPI) = doanh thu tinh tu Silver."""
    # BEGIN SOLUTION
    expected = silver_revenue_by_date(spark, cfg)
    broken = {}
    for table in ("daily_revenue_by_category", "daily_revenue_by_channel"):
        got = gold_revenue_by_date(spark, cfg, table)
        diff = {d.isoformat(): (got.get(d), expected.get(d))
                for d in set(got) | set(expected) if got.get(d) != expected.get(d)}
        if diff:
            broken[table] = diff
    # END SOLUTION
    if broken:
        raise QualityCheckError(f"revenue_reconciliation that bai (gold, silver): {broken}")
    return {"revenue_reconciliation": {d.isoformat(): v for d, v in sorted(expected.items())}}


# ---------------------------------------------------------------------------
# no_orphan_keys
# ---------------------------------------------------------------------------
# cot trong fact_sales -> (bang dim, cot khoa cua dim)
FACT_DIM_KEYS = [
    ("customer_id", "dim_customer", "customer_id"),
    ("product_id", "dim_product", "product_id"),
    ("order_date", "dim_date", "date"),
]


def check_no_orphan_keys(spark, cfg):
    """Moi khoa trong fact_sales phai ton tai trong bang dim tuong ung."""
    fact = read_gold(spark, cfg, "fact_sales")
    orphans = {}
    for fact_col, dim_table, dim_col in FACT_DIM_KEYS:
        # BEGIN SOLUTION
        dim = read_gold(spark, cfg, dim_table).select(F.col(dim_col).alias(fact_col))
        missing = fact.select(fact_col).distinct().join(dim, fact_col, "left_anti")
        n = missing.count()
        # END SOLUTION
        if n:
            sample = [str(r[0]) for r in missing.limit(5).collect()]
            orphans[f"{fact_col} -> {dim_table}"] = {"n": n, "vi_du": sample}
    if orphans:
        raise QualityCheckError(f"no_orphan_keys that bai: {orphans}")
    return {"no_orphan_keys": "ok"}
