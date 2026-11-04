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
