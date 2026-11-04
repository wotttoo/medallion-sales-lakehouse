"""Anh xa ten logic cua bang (vd 'bronze.orders_raw') sang duong dan Delta."""

from pathlib import Path

from common.config import resolve_path

# cac schema logic hop le va thu muc tuong ung duoi lakehouse/
SCHEMA_DIRS = {
    "bronze": "bronze",
    "silver": "silver",
    "quarantine": "quarantine",
    "gold": "gold",
    "_meta": "_meta",
}

# ten dung trong reading -> ten chuan
ALIASES = {
    "silver.orders_quarantine": "quarantine.orders",
    "silver.order_items_quarantine": "quarantine.order_items",
    "silver.customers_quarantine": "quarantine.customers",
    "silver.products_quarantine": "quarantine.products",
}


def canonical_name(name):
    """Doi ten bi danh (vd silver.orders_quarantine) ve ten chuan."""
    return ALIASES.get(name, name)


def table_path(name, cfg=None, lakehouse_root=None):
    """Tra ve Path cua bang Delta ung voi ten logic.

    Truyen cfg (lay paths.lakehouse) hoac lakehouse_root truc tiep.
    """
    name = canonical_name(name)
    parts = name.split(".")
    if len(parts) != 2 or parts[0] not in SCHEMA_DIRS or not parts[1]:
        raise ValueError(f"Ten bang khong hop le: {name}")
    if lakehouse_root is None:
        if cfg is None:
            raise ValueError("Can truyen cfg hoac lakehouse_root")
        lakehouse_root = resolve_path(cfg, "lakehouse")
    schema, table = parts
    return Path(lakehouse_root) / SCHEMA_DIRS[schema] / table


def table_uri(name, cfg=None, lakehouse_root=None):
    """Duong dan dang chuoi (dung cho spark.read/write)."""
    return str(table_path(name, cfg=cfg, lakehouse_root=lakehouse_root))


def schema_path(schema, cfg=None, lakehouse_root=None):
    """Thu muc chua moi bang cua mot schema logic (vd 'silver' -> lakehouse/silver)."""
    if schema not in SCHEMA_DIRS:
        raise ValueError(f"Schema khong hop le: {schema}")
    if lakehouse_root is None:
        if cfg is None:
            raise ValueError("Can truyen cfg hoac lakehouse_root")
        lakehouse_root = resolve_path(cfg, "lakehouse")
    return Path(lakehouse_root) / SCHEMA_DIRS[schema]
