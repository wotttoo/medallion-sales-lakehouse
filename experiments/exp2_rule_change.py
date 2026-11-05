"""Thi nghiem 2 - Doi quy tac: chay moi ngay voi v1, roi --rebuild voi v2.

Ky vong (so sanh voi answer key):
- so don cach ly giam dung bang so don chi dinh A1/A2 (khong dinh C1);
- doanh thu tang dung luong trong answer key;
- ORD-1002 van trong quarantine; _failed_rules v1 la
  'customer_id_not_null; order_date_valid_format; total_amount_numeric', v2 chi con
  'customer_id_not_null';
- Bronze khong doi (cung phien ban, cung so dong).
Chay: python experiments/exp2_rule_change.py [--keep]
"""

import sys

from _helpers import ENTITIES, Workspace, check, gold_revenue_by_date, print_table

import run_pipeline  # noqa: E402
from bronze_ingest import bronze_table_name, read_bronze  # noqa: E402
from common.paths import table_uri  # noqa: E402
from common.run_log import table_version  # noqa: E402
from common.spark import get_spark  # noqa: E402
from silver_transform import read_quarantine  # noqa: E402

V1_RULES = "customer_id_not_null; order_date_valid_format; total_amount_numeric"
V2_RULES = "customer_id_not_null"


def snapshot(spark, cfg):
    """So cach ly, doanh thu, ly do cua ORD-1002, trang thai Bronze."""
    q_orders = read_quarantine(spark, cfg, "orders")
    ord_1002 = q_orders.where("order_id = 'ORD-1002'").select("_failed_rules").collect()
    rules_by_count = {r["_failed_rules"]: r["count"]
                      for r in q_orders.groupBy("_failed_rules").count().collect()}
    return {
        "n_quarantined": {e: read_quarantine(spark, cfg, e).count() for e in ENTITIES},
        "quarantine_by_rules": rules_by_count,
        "revenue_by_date": gold_revenue_by_date(spark, cfg),
        "ord_1002_rules": [r["_failed_rules"] for r in ord_1002],
        "bronze_versions": {e: table_version(spark, table_uri(bronze_table_name(e), cfg)) for e in ENTITIES},
        "bronze_rows": {e: read_bronze(spark, cfg, e).count() for e in ENTITIES},
    }


def run(spark=None, keep=False):
    """Chay thi nghiem; tra ve dict ket qua."""
    spark = spark or get_spark()
    ws = Workspace("exp2", keep=keep)
    try:
        cfg_v1 = ws.cfg("v1")
        for date in cfg_v1["dates"]:
            run_pipeline.run_date(spark, cfg_v1, date, "all")
        v1 = snapshot(spark, cfg_v1)

        cfg_v2 = ws.cfg("v2")
        run_pipeline.run_rebuild(spark, cfg_v2)
        v2 = snapshot(spark, cfg_v2)

        rc = ws.key["rule_change"]
        total = lambda s: sum(s["revenue_by_date"].values())
        print_table("So ban ghi cach ly theo thuc the", ["thuc the", "v1", "v2", "giam"],
                    [[e, v1["n_quarantined"][e], v2["n_quarantined"][e],
                      v1["n_quarantined"][e] - v2["n_quarantined"][e]] for e in ENTITIES])
        print_table("quarantine.orders theo _failed_rules", ["_failed_rules", "v1", "v2"],
                    [[k, v1["quarantine_by_rules"].get(k, 0), v2["quarantine_by_rules"].get(k, 0)]
                     for k in sorted(set(v1["quarantine_by_rules"]) | set(v2["quarantine_by_rules"]))])
        print_table("Doanh thu theo order_date", ["order_date", "v1", "v2", "tang"],
                    [[d, v1["revenue_by_date"][d], v2["revenue_by_date"][d],
                      v2["revenue_by_date"][d] - v1["revenue_by_date"][d]] for d in v1["revenue_by_date"]]
                    + [["TONG", total(v1), total(v2), total(v2) - total(v1)]])

        decrease = v1["n_quarantined"]["orders"] - v2["n_quarantined"]["orders"]
        print("\nKiem tra:")
        check(decrease == rc["orders_only_A1_A2"],
              f"so don cach ly giam {decrease} = so don chi dinh A1/A2 trong answer key ({rc['orders_only_A1_A2']})")
        check(total(v2) - total(v1) == rc["revenue_increase"],
              f"doanh thu tang {total(v2) - total(v1)} = answer key ({rc['revenue_increase']})")
        check(total(v1) == rc["v1"]["total_revenue"] and total(v2) == rc["v2"]["total_revenue"],
              "tong doanh thu v1 va v2 khop answer key")
        check(v1["ord_1002_rules"] == [V1_RULES], f"ORD-1002 o v1: '{V1_RULES}'")
        check(v2["ord_1002_rules"] == [V2_RULES], f"ORD-1002 o v2 van bi cach ly: '{V2_RULES}'")
        check(v1["bronze_versions"] == v2["bronze_versions"] and v1["bronze_rows"] == v2["bronze_rows"],
              f"Bronze khong doi (phien ban {v1['bronze_versions']}, so dong {v1['bronze_rows']})")
        return {"v1": v1, "v2": v2, "quarantine_decrease": decrease,
                "revenue_increase": total(v2) - total(v1)}
    finally:
        ws.cleanup()


if __name__ == "__main__":
    run(keep="--keep" in sys.argv)
    print("\nThi nghiem 2: PASS")
