"""Bronze -> Silver + quarantine (SPEC 7.3).

Trinh tu cho moi thuc the, moi lo:
1. chuan hoa (ep kieu, trim, lowercase) theo rules_version;
2. ap TAT CA quy tac trong quality_rules.py;
3. tach nhanh: vi pham it nhat mot quy tac drop -> quarantine;
4. loai trung tren tap hop le (window theo khoa);
5. MERGE vao bang Silver theo khoa.
Thu tu thuc the: products -> customers -> orders -> order_items.
"""

from pyspark.sql import functions as F

# cot metadata tu Bronze, giu lai qua cac buoc
METADATA_COLUMNS = ["_ingest_time", "_source_file", "_batch_id"]


# ---------------------------------------------------------------------------
# Buoc 1: chuan hoa
# ---------------------------------------------------------------------------
def clean_string(col_name):
    """Trim; chuoi rong -> NULL."""
    c = F.trim(F.col(col_name))
    return F.when(c == "", F.lit(None)).otherwise(c)


def metadata(df):
    """Cac cot metadata co trong df (giu nguyen)."""
    return [F.col(c) for c in METADATA_COLUMNS if c in df.columns]


def normalize_products(df):
    """products: trim, list_price -> long."""
    return df.select(
        clean_string("product_id").alias("product_id"),
        clean_string("name").alias("name"),
        clean_string("category").alias("category"),
        clean_string("list_price").try_cast("long").alias("list_price"),
        *metadata(df),
    )


def normalize_customers(df):
    """customers: trim, email lowercase, phone rong -> NULL."""
    return df.select(
        clean_string("customer_id").alias("customer_id"),
        clean_string("name").alias("name"),
        F.lower(clean_string("email")).alias("email"),
        clean_string("phone").alias("phone"),
        clean_string("city").alias("city"),
        *metadata(df),
    )


ISO_DATE_PATTERN = r"^\d{4}-\d{2}-\d{2}$"
PLAIN_NUMBER_PATTERN = r"^\d+$"
TIMESTAMP_FORMAT = "yyyy-MM-dd HH:mm:ss"


def parse_date(col, pattern, fmt):
    """Parse ngay neu khop regex; khong khop hoac ngay khong ton tai -> NULL."""
    return F.when(col.rlike(pattern), F.to_date(F.try_to_timestamp(col, F.lit(fmt))))


def parse_order_date(col_name, rules_version):
    """order_date -> date. v1: chi YYYY-MM-DD."""
    c = clean_string(col_name)
    return parse_date(c, ISO_DATE_PATTERN, "yyyy-MM-dd")


def parse_amount(col_name, rules_version):
    """total_amount -> long. v1: chi so thuan."""
    c = clean_string(col_name)
    return F.when(c.rlike(PLAIN_NUMBER_PATTERN), c.try_cast("long"))


def normalize_orders(df, rules_version):
    """orders: ep kieu theo rules_version, lowercase status/channel."""
    promo = clean_string("promo_code") if "promo_code" in df.columns else F.lit(None).cast("string")
    return df.select(
        clean_string("order_id").alias("order_id"),
        clean_string("customer_id").alias("customer_id"),
        parse_order_date("order_date", rules_version).alias("order_date"),
        parse_amount("total_amount", rules_version).alias("total_amount"),
        F.lower(clean_string("status")).alias("status"),
        F.lower(clean_string("channel")).alias("channel"),
        F.try_to_timestamp(clean_string("updated_at"), F.lit(TIMESTAMP_FORMAT)).alias("updated_at"),
        promo.alias("promo_code"),
        *metadata(df),
    )
