"""Silver -> Gold (SPEC 7.4).

Quy tac nghiep vu:
- doanh thu = tong line_amount cua cac dong thuoc don co status != 'cancelled';
- doanh thu gan theo order_date (khong theo ngay nap).
Bang dim ghi de toan bang; fact va KPI theo ngay ghi de theo phan vung (replaceWhere)
cho moi order_date bi anh huong boi lo.
"""

from delta.tables import DeltaTable
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


# ---------------------------------------------------------------------------
# Ghi de theo phan vung order_date
# ---------------------------------------------------------------------------
def date_predicate(dates):
    """Bieu thuc replaceWhere cho danh sach ngay."""
    return "order_date IN (" + ", ".join(f"DATE'{d.isoformat()}'" for d in dates) + ")"


def replace_dates(spark, cfg, table, df, dates):
    """Ghi de dung cac phan vung order_date trong `dates`; tra ve phien ban Delta.

    Ngay khong con dong nao (vd moi don bi huy) cung bi xoa sach trong bang.
    """
    path = gold_path(cfg, table)
    # BEGIN SOLUTION
    df = df.where(F.col("order_date").isin(dates))
    if not DeltaTable.isDeltaTable(spark, path):
        df.write.format("delta").partitionBy("order_date").save(path)
    else:
        (df.write.format("delta").mode("overwrite")
           .option("replaceWhere", date_predicate(dates)).save(path))
    # END SOLUTION
    return table_version(spark, path)


# ---------------------------------------------------------------------------
# fact_sales: grain = mot dong san pham trong mot don
# ---------------------------------------------------------------------------
FACT_COLUMNS = ["order_id", "product_id", "customer_id", "order_date", "channel", "status",
                "quantity", "unit_price", "line_amount"]


def build_fact_sales(spark, cfg):
    """Noi silver.order_items voi silver.orders, dung khoa tu nhien."""
    items = read_silver(spark, cfg, "order_items")
    orders = read_silver(spark, cfg, "orders").select(
        "order_id", "customer_id", "order_date", "channel", "status")
    return items.join(orders, "order_id").select(FACT_COLUMNS)


def write_fact_sales(spark, cfg, dates):
    """Ghi de fact_sales cho cac ngay bi anh huong."""
    return replace_dates(spark, cfg, "fact_sales", build_fact_sales(spark, cfg), dates)


# ---------------------------------------------------------------------------
# KPI doanh thu theo ngay
# ---------------------------------------------------------------------------
def revenue_lines(spark, cfg):
    """Dong fact tinh vao doanh thu: don khong bi huy."""
    # BEGIN SOLUTION
    return read_gold(spark, cfg, "fact_sales").where(F.col("status") != "cancelled")
    # END SOLUTION


def aggregate_revenue(lines, *group_cols):
    """revenue = tong line_amount; n_orders = so don khac nhau."""
    return lines.groupBy(*group_cols).agg(
        F.sum("line_amount").alias("revenue"),
        F.countDistinct("order_id").alias("n_orders"))


def build_revenue_by_category(spark, cfg):
    """daily_revenue_by_category: order_date, category, revenue, n_orders."""
    products = read_gold(spark, cfg, "dim_product").select("product_id", "category")
    lines = revenue_lines(spark, cfg).join(products, "product_id")
    return aggregate_revenue(lines, "order_date", "category").select(
        "order_date", "category", "revenue", "n_orders")


def write_revenue_by_category(spark, cfg, dates):
    """Ghi de KPI theo category cho cac ngay bi anh huong."""
    return replace_dates(spark, cfg, "daily_revenue_by_category",
                         build_revenue_by_category(spark, cfg), dates)
