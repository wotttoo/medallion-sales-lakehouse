"""Quy tac chat luong muc ban ghi, khai bao dang du lieu (SPEC 7.3).

Moi quy tac:
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

RULES = [
    {"name": "customer_id_not_null", "entity": "orders",
     "condition": "customer_id IS NOT NULL", "action": "drop", "stage": "main"},
    {"name": "order_date_valid_format", "entity": "orders",
     "condition": "order_date IS NOT NULL", "action": "drop", "stage": "main"},
    {"name": "total_amount_numeric", "entity": "orders",
     "condition": "total_amount IS NOT NULL", "action": "drop", "stage": "main"},
    {"name": "status_in_allowed_set", "entity": "orders",
     "condition": "status IN ('" + "', '".join(ALLOWED_STATUS) + "')",
     "action": "drop", "stage": "main"},
    {"name": "quantity_positive", "entity": "order_items",
     "condition": "quantity > 0", "action": "drop", "stage": "main"},
    {"name": "product_exists", "entity": "order_items",
     "condition": "_product_exists", "action": "drop", "stage": "main"},
    {"name": "order_exists", "entity": "order_items",
     "condition": "_order_exists", "action": "drop", "stage": "main"},
    {"name": "phone_not_null", "entity": "customers",
     "condition": "phone IS NOT NULL", "action": "warn", "stage": "main"},
    {"name": "total_amount_matches_items", "entity": "orders",
     "condition": "total_amount = _items_total", "action": "warn", "stage": "after_items"},
]


def rules_for(entity, stage="main", action=None):
    """Danh sach quy tac cua thuc the theo stage (va action neu truyen vao)."""
    return [r for r in RULES
            if r["entity"] == entity and r["stage"] == stage
            and (action is None or r["action"] == action)]


def rule_names(entity=None):
    """Ten cac quy tac (tuy chon loc theo thuc the), dung thu tu khai bao."""
    return [r["name"] for r in RULES if entity is None or r["entity"] == entity]
