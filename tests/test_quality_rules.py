"""Test khai bao quy tac chat luong."""

import quality_rules as qr

REQUIRED = {
    "customer_id_not_null": ("orders", "drop"),
    "order_date_valid_format": ("orders", "drop"),
    "total_amount_numeric": ("orders", "drop"),
    "status_in_allowed_set": ("orders", "drop"),
    "quantity_positive": ("order_items", "drop"),
    "product_exists": ("order_items", "drop"),
    "order_exists": ("order_items", "drop"),
    "phone_not_null": ("customers", "warn"),
    "total_amount_matches_items": ("orders", "warn"),
}


def test_required_rules_exact():
    """Du 9 quy tac bat buoc, dung ten, thuc the va action."""
    got = {r["name"]: (r["entity"], r["action"]) for r in qr.all_rules()}
    assert got == REQUIRED


def test_rules_are_data():
    """Moi quy tac la dict co du truong, condition la chuoi bieu thuc."""
    for r in qr.all_rules():
        assert set(r) == {"name", "entity", "condition", "action", "stage"}
        assert isinstance(r["condition"], str) and r["condition"].strip()
        assert r["action"] in ("warn", "drop")
        assert r["stage"] in ("main", "after_items")


def test_orders_rule_order_matches_reading():
    """Thu tu cho ra 'customer_id_not_null; order_date_valid_format; total_amount_numeric'."""
    names = [r["name"] for r in qr.rules_for("orders", action="drop")]
    assert names[:3] == ["customer_id_not_null", "order_date_valid_format", "total_amount_numeric"]


def test_amount_match_rule_runs_after_items():
    """total_amount_matches_items chi ap sau khi co order_items."""
    assert [r["name"] for r in qr.rules_for("orders", stage="after_items")] == \
        ["total_amount_matches_items"]
    assert "total_amount_matches_items" not in [r["name"] for r in qr.rules_for("orders")]


def test_conditions_evaluate_in_spark(spark):
    """Moi condition la bieu thuc Spark hop le tren mot dong mau."""
    from pyspark.sql import functions as F
    df = spark.createDataFrame(
        [("C-1", "2026-10-19", 1, "pending", 2, True, True, "0900000000", 1)],
        "customer_id string, order_date string, total_amount long, status string, "
        "quantity int, _product_exists boolean, _order_exists boolean, phone string, "
        "_items_total long")
    row = df.select([F.expr(r["condition"]).alias(r["name"]) for r in qr.all_rules()]).collect()[0]
    assert all(row[r["name"]] is True for r in qr.all_rules())
