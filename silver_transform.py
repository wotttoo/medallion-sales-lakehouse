"""Bronze -> Silver + quarantine (SPEC 7.3).

Trinh tu cho moi thuc the, moi lo:
1. chuan hoa (ep kieu, trim, lowercase) theo rules_version;
2. ap TAT CA quy tac trong quality_rules.py;
3. tach nhanh: vi pham it nhat mot quy tac drop -> quarantine;
4. loai trung tren tap hop le (window theo khoa);
5. MERGE vao bang Silver theo khoa.
Thu tu thuc the: products -> customers -> orders -> order_items.
"""

import shutil

from delta.tables import DeltaTable
from pyspark.sql import Window
from pyspark.sql import functions as F

from bronze_ingest import read_bronze
from common.paths import schema_path, table_uri
from common.run_log import read_ingested_files, successful_batches, table_version
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
    """Cac cot metadata co trong df (giu nguyen), ke ca ban goc _raw neu co."""
    return [F.col(c) for c in METADATA_COLUMNS + ["_raw"] if c in df.columns]


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


# ---------------------------------------------------------------------------
# Quarantine: giu nguyen gia tri goc (string) + ly do
# ---------------------------------------------------------------------------
RAW_COLUMNS = {
    "products": ["product_id", "name", "category", "list_price"],
    "customers": ["customer_id", "name", "email", "phone", "city"],
    "orders": ["order_id", "customer_id", "order_date", "total_amount", "status",
               "channel", "updated_at", "promo_code"],
    "order_items": ["order_id", "product_id", "quantity", "unit_price"],
}


def quarantine_table_name(entity):
    """Ten logic bang quarantine (reading goi la silver.<entity>_quarantine)."""
    return f"quarantine.{entity}"


def attach_raw(bronze_df, entity):
    """Dong goi gia tri goc (string) vao cot struct _raw truoc khi chuan hoa."""
    fields = [(F.col(c) if c in bronze_df.columns else F.lit(None)).cast("string").alias(c)
              for c in RAW_COLUMNS[entity]]
    return bronze_df.withColumn("_raw", F.struct(*fields))


def to_quarantine_rows(quarantined):
    """Cot quarantine: cot goc string + _ingest_time, _source_file, _batch_id,
    _failed_rules, _quarantined_at."""
    return quarantined.select(
        "_raw.*", "_ingest_time", "_source_file", "_batch_id", "_failed_rules",
        F.current_timestamp().alias("_quarantined_at"))


def write_quarantine(spark, cfg, entity, quarantined, batch_id):
    """Ghi ban ghi cach ly cua lo vao quarantine.<entity>; tra ve phien ban Delta.

    Ghi de dung phan cua lo (replaceWhere _batch_id) nen chay lai mot lo khong nhan doi.
    """
    rows = to_quarantine_rows(quarantined)
    path = table_uri(quarantine_table_name(entity), cfg)
    ensure_delta_table(spark, path, rows.schema)
    (rows.write.format("delta").mode("overwrite")
         .option("replaceWhere", f"_batch_id = '{batch_id}'")
         .save(path))
    return table_version(spark, path)


def read_quarantine(spark, cfg, entity):
    """Doc bang quarantine cua thuc the."""
    return spark.read.format("delta").load(table_uri(quarantine_table_name(entity), cfg))


# ---------------------------------------------------------------------------
# Luong Silver cho mot lo
# ---------------------------------------------------------------------------
ENTITY_ORDER = ["products", "customers", "orders", "order_items"]


def pending_batches(spark, cfg):
    """Cac _batch_id da co trong Bronze nhung Silver chua xu ly thanh cong (tang dan)."""
    all_batches = {r["_batch_id"] for r in
                   read_ingested_files(spark, cfg).select("_batch_id").distinct().collect()}
    return sorted(all_batches - successful_batches(spark, cfg, "silver"))


def add_reference_flags(spark, cfg, df):
    """order_items: them _product_exists, _order_exists tu Silver hien tai."""
    df = add_exists_flag(df, read_silver(spark, cfg, "products"), "product_id", "_product_exists")
    return add_exists_flag(df, read_silver(spark, cfg, "orders"), "order_id", "_order_exists")


def process_entity(spark, cfg, entity, batch_id):
    """Chuan hoa -> quy tac -> tach nhanh -> loai trung -> MERGE + quarantine cho mot thuc the.

    Tra ve (thong ke, so canh bao, phien ban Delta da ghi, tap hop le da loai trung).
    Tap hop le dang duoc cache: nguoi goi unpersist khi dung xong.
    """
    bronze = read_bronze(spark, cfg, entity, batch_id)
    df = normalize(attach_raw(bronze, entity), entity, cfg["rules_version"])
    if entity == "order_items":
        df = add_reference_flags(spark, cfg, df)
    checked, warns = apply_rules(df, entity)
    checked = checked.cache()
    valid, quarantined = split_valid_quarantine(checked)
    deduped = deduplicate(valid, entity).cache()

    n_input = checked.count()
    n_quarantined = quarantined.count()
    n_valid_rows = n_input - n_quarantined
    n_valid = deduped.count()
    stats = {"n_input": n_input, "n_valid": n_valid, "n_quarantined": n_quarantined,
             "n_duplicates_removed": n_valid_rows - n_valid}

    versions = {
        silver_table_name(entity): merge_into_silver(spark, cfg, entity, deduped),
        quarantine_table_name(entity): write_quarantine(spark, cfg, entity, quarantined, batch_id),
    }
    checked.unpersist()
    return stats, warns, versions, deduped


def check_amount_matches_items(spark, cfg, batch_orders):
    """Quy tac warn total_amount_matches_items: chay sau khi order_items da vao Silver.

    batch_orders: DataFrame co cot order_id (cac don hop le cua lo).
    So total_amount trong silver.orders voi tong line_amount trong silver.order_items.
    """
    items_total = (read_silver(spark, cfg, "order_items").groupBy("order_id")
                   .agg(F.sum("line_amount").alias("_items_total")))
    orders = (read_silver(spark, cfg, "orders")
              .join(batch_orders.select("order_id").distinct(), "order_id")
              .join(items_total, "order_id", "left")
              .withColumn("_items_total", F.coalesce(F.col("_items_total"), F.lit(0))))
    _, warns = apply_rules(orders, "orders", stage="after_items")
    return warns


def process_batch(spark, cfg, batch_id):
    """Xu ly mot lo Bronze qua Silver theo thu tu products -> customers -> orders -> order_items.

    Tra ve dict: _batch_id, entities (thong ke tung thuc the), n_warn_by_rule,
    delta_versions_written, input_files, tong n_*.
    """
    result = {"_batch_id": batch_id, "entities": {}, "n_warn_by_rule": {},
              "delta_versions_written": {}}
    batch_orders = None
    for entity in ENTITY_ORDER:
        stats, warns, versions, deduped = process_entity(spark, cfg, entity, batch_id)
        if entity == "orders":
            batch_orders = deduped        # giu lai cho quy tac sau order_items
        else:
            deduped.unpersist()
        result["entities"][entity] = stats
        result["n_warn_by_rule"].update(warns)
        result["delta_versions_written"].update(versions)
    result["n_warn_by_rule"].update(check_amount_matches_items(spark, cfg, batch_orders))
    batch_orders.unpersist()
    for col in ("n_input", "n_valid", "n_quarantined", "n_duplicates_removed"):
        result[col] = sum(s[col] for s in result["entities"].values())
    result["input_files"] = sorted(
        r["file_name"] for r in read_ingested_files(spark, cfg)
        .where(F.col("_batch_id") == batch_id).select("file_name").collect())
    return result


# ---------------------------------------------------------------------------
# Rebuild: dung lai Silver/quarantine/Gold tu Bronze
# ---------------------------------------------------------------------------
# chi --rebuild duoc xoa cac lop nay; Bronze va landing KHONG BAO GIO bi xoa
REBUILD_SCHEMAS = ("silver", "quarantine", "gold")


def all_batches(spark, cfg):
    """Moi _batch_id trong Bronze, theo thu tu nap (b-0001, b-0002, ...)."""
    ids = {r["_batch_id"] for r in
           read_ingested_files(spark, cfg).select("_batch_id").distinct().collect()}
    return sorted(ids, key=lambda b: int(b.split("-")[1]))


def drop_derived_layers(cfg):
    """Xoa thu muc silver/, quarantine/, gold/ cua lakehouse (khong dung Bronze, _meta)."""
    removed = []
    for schema in REBUILD_SCHEMAS:
        path = schema_path(schema, cfg)
        if path.exists():
            shutil.rmtree(path)
            removed.append(schema)
    return removed
