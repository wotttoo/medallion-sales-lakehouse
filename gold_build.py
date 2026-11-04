"""Silver -> Gold (SPEC 7.4).

Quy tac nghiep vu:
- doanh thu = tong line_amount cua cac dong thuoc don co status != 'cancelled';
- doanh thu gan theo order_date (khong theo ngay nap).
Bang dim ghi de toan bang; fact va KPI theo ngay ghi de theo phan vung (replaceWhere)
cho moi order_date bi anh huong boi lo.
"""

from pyspark.sql import functions as F

from bronze_ingest import read_bronze
from common.paths import table_uri
from common.run_log import table_version
from silver_transform import read_silver


def gold_path(cfg, table):
    """Duong dan bang gold.<table>."""
    return table_uri(f"gold.{table}", cfg)


def read_gold(spark, cfg, table):
    """Doc bang Gold."""
    return spark.read.format("delta").load(gold_path(cfg, table))


def overwrite_table(spark, cfg, table, df):
    """Ghi de toan bang (dung cho bang nho); tra ve phien ban Delta."""
    path = gold_path(cfg, table)
    (df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(path))
    return table_version(spark, path)


# ---------------------------------------------------------------------------
# Bang dim
# ---------------------------------------------------------------------------
def build_dim_customer(spark, cfg):
    """dim_customer tu silver.customers (khoa tu nhien customer_id)."""
    return read_silver(spark, cfg, "customers").select(
        "customer_id", "name", "email", "phone", "city")


def build_dim_product(spark, cfg):
    """dim_product tu silver.products, co category."""
    return read_silver(spark, cfg, "products").select(
        "product_id", "name", "category", "list_price")


def build_dim_date(spark, cfg):
    """dim_date: moi ngay tu order_date nho nhat den lon nhat trong silver.orders."""
    bounds = read_silver(spark, cfg, "orders").agg(
        F.min("order_date").alias("start"), F.max("order_date").alias("end"))
    days = bounds.select(F.explode(F.sequence("start", "end")).alias("date"))
    return days.select(
        "date",
        F.date_format("date", "EEEE").alias("day_of_week"),
        F.month("date").alias("month"),
        F.quarter("date").alias("quarter"),
        F.year("date").alias("year"),
    )


def write_dims(spark, cfg):
    """Ghi de ba bang dim; tra ve dict ten bang -> phien ban Delta."""
    return {
        "gold.dim_customer": overwrite_table(spark, cfg, "dim_customer", build_dim_customer(spark, cfg)),
        "gold.dim_product": overwrite_table(spark, cfg, "dim_product", build_dim_product(spark, cfg)),
        "gold.dim_date": overwrite_table(spark, cfg, "dim_date", build_dim_date(spark, cfg)),
    }


# ---------------------------------------------------------------------------
# Ngay bi anh huong boi mot lo
# ---------------------------------------------------------------------------
def affected_dates(spark, cfg, batch_id):
    """Tap order_date (theo silver.orders hien tai) cua moi don co dong trong lo.

    Gom ca don den muon (order_date cu hon ngay xu ly) va don gui lai de cap nhat
    trang thai, vi vay khong chi la processing_date.
    """
    # BEGIN SOLUTION
    order_ids = (read_bronze(spark, cfg, "orders", batch_id).select("order_id")
                 .unionByName(read_bronze(spark, cfg, "order_items", batch_id).select("order_id"))
                 .select(F.trim("order_id").alias("order_id")).distinct())
    rows = (read_silver(spark, cfg, "orders").join(order_ids, "order_id")
            .select("order_date").distinct().collect())
    # END SOLUTION
    return sorted(r["order_date"] for r in rows if r["order_date"] is not None)


def all_order_dates(spark, cfg):
    """Moi order_date dang co trong silver.orders (dung khi dung lai Gold toan bo)."""
    rows = read_silver(spark, cfg, "orders").select("order_date").distinct().collect()
    return sorted(r["order_date"] for r in rows if r["order_date"] is not None)
