"""Bronze -> Silver + quarantine (SPEC 7.3).

Trinh tu cho moi thuc the, moi lo:
1. chuan hoa (ep kieu, trim, lowercase) theo rules_version;
2. ap TAT CA quy tac trong quality_rules.py;
3. tach nhanh: vi pham it nhat mot quy tac drop -> quarantine;
4. loai trung tren tap hop le (window theo khoa);
5. MERGE vao bang Silver theo khoa.
Thu tu thuc the: products -> customers -> orders -> order_items.
"""

from delta.tables import DeltaTable
from pyspark.sql import Window
from pyspark.sql import functions as F

from common.paths import table_uri
from common.run_log import table_version
from quality_rules import rules_for

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
DMY_DATE_PATTERN = r"^\d{2}/\d{2}/\d{4}$"
PLAIN_NUMBER_PATTERN = r"^\d+$"
VND_STRING_PATTERN = r"^\d{1,3}(\.\d{3})*\s*(đ|₫|vnd|VND)$"
TIMESTAMP_FORMAT = "yyyy-MM-dd HH:mm:ss"


def parse_date(col, pattern, fmt):
    """Parse ngay neu khop regex; khong khop hoac ngay khong ton tai -> NULL."""
    return F.when(col.rlike(pattern), F.to_date(F.try_to_timestamp(col, F.lit(fmt))))


def parse_order_date(col_name, rules_version):
    """order_date -> date. v1: chi YYYY-MM-DD; v2: them DD/MM/YYYY."""
    c = clean_string(col_name)
    # BEGIN SOLUTION
    iso = parse_date(c, ISO_DATE_PATTERN, "yyyy-MM-dd")
    if rules_version == "v1":
        return iso
    return F.coalesce(iso, parse_date(c, DMY_DATE_PATTERN, "dd/MM/yyyy"))
    # END SOLUTION


def parse_amount(col_name, rules_version):
    """total_amount -> long. v1: chi so thuan; v2: them chuoi '150.000đ'."""
    c = clean_string(col_name)
    # BEGIN SOLUTION
    plain = F.when(c.rlike(PLAIN_NUMBER_PATTERN), c.try_cast("long"))
    if rules_version == "v1":
        return plain
    digits = F.regexp_replace(c, r"[^0-9]", "")
    vnd = F.when(c.rlike(VND_STRING_PATTERN), digits.try_cast("long"))
    return F.coalesce(plain, vnd)
    # END SOLUTION


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


def normalize_order_items(df):
    """order_items: ep quantity -> int, unit_price -> long, them line_amount."""
    quantity = clean_string("quantity").try_cast("int")
    unit_price = clean_string("unit_price").try_cast("long")
    return df.select(
        clean_string("order_id").alias("order_id"),
        clean_string("product_id").alias("product_id"),
        quantity.alias("quantity"),
        unit_price.alias("unit_price"),
        (quantity.cast("long") * unit_price).alias("line_amount"),
        *metadata(df),
    )


def normalize(df, entity, rules_version):
    """Goi ham chuan hoa ung voi thuc the."""
    if entity == "products":
        return normalize_products(df)
    if entity == "customers":
        return normalize_customers(df)
    if entity == "orders":
        return normalize_orders(df, rules_version)
    if entity == "order_items":
        return normalize_order_items(df)
    raise ValueError(f"Thuc the khong hop le: {entity}")


# ---------------------------------------------------------------------------
# Buoc 2-3: ap quy tac va tach nhanh
# ---------------------------------------------------------------------------
def add_exists_flag(df, ref_df, key, flag_name):
    """Them cot boolean flag_name: khoa `key` co trong ref_df hay khong."""
    ref = ref_df.select(key).distinct().withColumn(flag_name, F.lit(True))
    return (df.join(ref, on=key, how="left")
              .withColumn(flag_name, F.coalesce(F.col(flag_name), F.lit(False))))


def rule_passed(rule):
    """Cot boolean: TRUE neu dat quy tac (NULL tinh la vi pham)."""
    # BEGIN SOLUTION
    return F.coalesce(F.expr(rule["condition"]), F.lit(False))
    # END SOLUTION


def failed_rules_column(rules):
    """Chuoi ten cac quy tac drop bi vi pham, noi bang '; ' (NULL neu khong vi pham)."""
    # BEGIN SOLUTION
    names = [F.when(~rule_passed(r), F.lit(r["name"])) for r in rules if r["action"] == "drop"]
    if not names:
        return F.lit(None).cast("string")
    joined = F.concat_ws("; ", *names)          # concat_ws bo qua NULL
    return F.when(joined != "", joined)
    # END SOLUTION


def count_warnings(df, rules):
    """Dem so ban ghi vi pham tung quy tac warn (tren moi ban ghi dau vao)."""
    warn_rules = [r for r in rules if r["action"] == "warn"]
    if not warn_rules:
        return {}
    row = df.agg(*[F.sum(F.when(~rule_passed(r), 1).otherwise(0)).alias(r["name"])
                   for r in warn_rules]).collect()[0]
    return {r["name"]: int(row[r["name"]] or 0) for r in warn_rules}


def apply_rules(df, entity, stage="main"):
    """Danh gia TAT CA quy tac cua thuc the, them cot _failed_rules.

    Tra ve (df_co_failed_rules, so_canh_bao_theo_quy_tac).
    """
    rules = rules_for(entity, stage)
    checked = df.withColumn("_failed_rules", failed_rules_column(rules))
    return checked, count_warnings(df, rules)


def split_valid_quarantine(checked):
    """Tach nhanh: _failed_rules NULL -> hop le; nguoc lai -> quarantine."""
    # BEGIN SOLUTION
    valid = checked.where(F.col("_failed_rules").isNull()).drop("_failed_rules")
    quarantined = checked.where(F.col("_failed_rules").isNotNull())
    # END SOLUTION
    return valid, quarantined


# ---------------------------------------------------------------------------
# Buoc 4: loai trung
# ---------------------------------------------------------------------------
ENTITY_KEYS = {
    "products": ["product_id"],
    "customers": ["customer_id"],
    "orders": ["order_id"],
    "order_items": ["order_id", "product_id"],
}


def dedup_order(df):
    """Thu tu uu tien trong window: updated_at (neu co), _ingest_time, _source_file - giam dan."""
    order = []
    if "updated_at" in df.columns:
        order.append(F.col("updated_at").desc_nulls_last())
    order += [F.col("_ingest_time").desc(), F.col("_source_file").desc()]
    return order


def deduplicate(valid, entity):
    """Giu mot dong moi khoa: dong dung dau theo dedup_order."""
    # BEGIN SOLUTION
    window = Window.partitionBy(*ENTITY_KEYS[entity]).orderBy(*dedup_order(valid))
    return (valid.withColumn("_rn", F.row_number().over(window))
                 .where(F.col("_rn") == 1)
                 .drop("_rn"))
    # END SOLUTION


# ---------------------------------------------------------------------------
# Buoc 5: MERGE vao Silver
# ---------------------------------------------------------------------------
SILVER_COLUMNS = {
    "products": ["product_id", "name", "category", "list_price"],
    "customers": ["customer_id", "name", "email", "phone", "city"],
    "orders": ["order_id", "customer_id", "order_date", "total_amount", "status",
               "channel", "updated_at", "promo_code"],
    "order_items": ["order_id", "product_id", "quantity", "unit_price", "line_amount"],
}


def silver_table_name(entity):
    """Ten logic bang Silver (vd silver.orders)."""
    return f"silver.{entity}"


def silver_columns(entity):
    """Cot cua bang Silver: cot nghiep vu + metadata nguon goc."""
    return SILVER_COLUMNS[entity] + METADATA_COLUMNS


def ensure_delta_table(spark, path, schema):
    """Tao bang Delta rong voi schema neu chua co."""
    if not DeltaTable.isDeltaTable(spark, path):
        spark.createDataFrame([], schema).write.format("delta").save(path)


def merge_into_silver(spark, cfg, entity, deduped):
    """MERGE tap da loai trung vao silver.<entity> theo khoa; tra ve phien ban Delta.

    orders: chi update khi updated_at nguon moi hon hoac bang (ban cu khong ghi de ban moi).
    Thuc the khac: lo moi nhat thang.
    """
    source = deduped.select(silver_columns(entity))
    path = table_uri(silver_table_name(entity), cfg)
    ensure_delta_table(spark, path, source.schema)
    # BEGIN SOLUTION
    condition = " AND ".join(f"t.{k} = s.{k}" for k in ENTITY_KEYS[entity])
    merge = DeltaTable.forPath(spark, path).alias("t").merge(source.alias("s"), condition)
    if entity == "orders":
        merge = merge.whenMatchedUpdateAll(condition="s.updated_at >= t.updated_at")
    else:
        merge = merge.whenMatchedUpdateAll()
    merge.whenNotMatchedInsertAll().execute()
    # END SOLUTION
    return table_version(spark, path)


def read_silver(spark, cfg, entity):
    """Doc bang Silver cua thuc the."""
    return spark.read.format("delta").load(table_uri(silver_table_name(entity), cfg))
