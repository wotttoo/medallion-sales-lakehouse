"""Quy tac chat luong muc ban ghi, khai bao dang du lieu (SPEC 7.3).

Moi quy tac (dict do all_rules() tra ve):
- name: ten quy tac (khop reading), ghi vao _failed_rules;
- entity: thuc the ap dung;
- condition: bieu thuc cot Spark (cu phap SQL) - TRUE nghia la ban ghi DAT;
  NULL duoc tinh la vi pham;
- action: 'drop' (vi pham -> quarantine) hoac 'warn' (chi dem vao run log);
- stage: 'main' (ap khi xu ly thuc the) hoac 'after_items' (ap sau khi da
  co order_items trong Silver).

Thu tu khai bao = thu tu ten quy tac trong _failed_rules.
Cot bat dau bang '_' (vd _product_exists) do silver_transform them vao truoc khi ap quy tac.
"""

ALLOWED_STATUS = ("pending", "shipping", "delivered", "cancelled")

# (name, entity, action, stage) - dieu kien nam o rule_condition()
RULE_SPECS = [
    ("customer_id_not_null", "orders", "drop", "main"),
    ("order_date_valid_format", "orders", "drop", "main"),
    ("total_amount_numeric", "orders", "drop", "main"),
    ("status_in_allowed_set", "orders", "drop", "main"),
    ("quantity_positive", "order_items", "drop", "main"),
    ("product_exists", "order_items", "drop", "main"),
    ("order_exists", "order_items", "drop", "main"),
    ("phone_not_null", "customers", "warn", "main"),
    ("total_amount_matches_items", "orders", "warn", "after_items"),
]


def rule_condition(name):
    """Bieu thuc SQL cua quy tac `name` (TRUE = ban ghi dat), ap tren cot da chuan hoa."""
    # BEGIN SOLUTION
    conditions = {
        "customer_id_not_null": "customer_id IS NOT NULL",
        "order_date_valid_format": "order_date IS NOT NULL",      # NULL = khong parse duoc
        "total_amount_numeric": "total_amount IS NOT NULL",
        "status_in_allowed_set": "status IN ('" + "', '".join(ALLOWED_STATUS) + "')",
        "quantity_positive": "quantity > 0",
        "product_exists": "_product_exists",
        "order_exists": "_order_exists",
        "phone_not_null": "phone IS NOT NULL",
        "total_amount_matches_items": "total_amount = _items_total",
    }
    return conditions[name]
    # END SOLUTION


def all_rules():
    """Danh sach quy tac dang du lieu (list dict), dung thu tu khai bao."""
    return [{"name": name, "entity": entity, "condition": rule_condition(name),
             "action": action, "stage": stage}
            for name, entity, action, stage in RULE_SPECS]


def rules_for(entity, stage="main", action=None):
    """Danh sach quy tac cua thuc the theo stage (va action neu truyen vao)."""
    return [r for r in all_rules()
            if r["entity"] == entity and r["stage"] == stage
            and (action is None or r["action"] == action)]


def rule_names(entity=None):
    """Ten cac quy tac (tuy chon loc theo thuc the), dung thu tu khai bao."""
    return [name for name, e, _a, _s in RULE_SPECS if entity is None or e == entity]
