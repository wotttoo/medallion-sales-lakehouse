"""Thi nghiem 3 - Time travel tren silver.orders.

1. In DESCRIBE HISTORY cua silver.orders sau khi chay ca 3 ngay.
2. Doc phien ban truoc va sau lo ngay 2 (versionAsOf) de chi ra cac don T1 doi trang thai.
3. Co y ghi mot lo sai vao silver.orders, roi RESTORE ve phien ban truoc va assert bang khop lai.
Chay: python experiments/exp3_time_travel.py [--keep]
"""

import json
import sys

from _helpers import Workspace, check, print_table

import run_pipeline  # noqa: E402
from common.paths import table_uri  # noqa: E402
from common.run_log import read_run_log  # noqa: E402
from common.spark import get_spark  # noqa: E402

HISTORY_COLUMNS = ["version", "timestamp", "operation", "operationMetrics"]


def history_rows(spark, path):
    """DESCRIBE HISTORY cua bang Delta theo duong dan, sap theo version tang dan."""
    df = spark.sql(f"DESCRIBE HISTORY delta.`{path}`")
    rows = []
    for r in df.orderBy("version").collect():
        metrics = r["operationMetrics"] or {}
        short = {k: metrics[k] for k in ("numTargetRowsInserted", "numTargetRowsUpdated",
                                         "numOutputRows", "numRestoredFiles") if k in metrics}
        rows.append({"version": r["version"], "timestamp": str(r["timestamp"])[:19],
                     "operation": r["operation"], "metrics": short})
    return rows


def silver_version_of_batch(spark, cfg, batch_id):
    """Phien ban silver.orders do lo batch_id ghi ra (lay tu run log)."""
    row = (read_run_log(spark, cfg)
           .where(f"layer = 'silver' AND status = 'success' AND _batch_id = '{batch_id}'")
           .orderBy("run_started_at").collect()[-1])
    return json.loads(row["delta_versions_written"])["silver.orders"]


def sorted_rows(df):
    """Noi dung bang (sap xep) de so sanh."""
    return sorted(tuple(r) for r in df.collect())


def run(spark=None, keep=False):
    """Chay thi nghiem; tra ve dict ket qua."""
    spark = spark or get_spark()
    ws = Workspace("exp3", keep=keep)
    try:
        cfg = ws.cfg("v2")
        for date in cfg["dates"]:
            run_pipeline.run_date(spark, cfg, date, "all")
        path = table_uri("silver.orders", cfg)

        history = history_rows(spark, path)
        print_table("DESCRIBE HISTORY silver.orders", ["version", "timestamp", "operation", "metrics"],
                    [[h["version"], h["timestamp"], h["operation"], h["metrics"]] for h in history])

        # 2. truoc / sau lo ngay 2
        v_day1 = silver_version_of_batch(spark, cfg, "b-0001")
        v_day2 = silver_version_of_batch(spark, cfg, "b-0002")
        before = spark.read.format("delta").option("versionAsOf", v_day1).load(path)
        after = spark.read.format("delta").option("versionAsOf", v_day2).load(path)
        changed = (before.alias("b").join(after.alias("a"), "order_id")
                   .where("b.status <> a.status")
                   .selectExpr("order_id", "b.status AS status_before", "a.status AS status_after",
                               "b.updated_at AS updated_before", "a.updated_at AS updated_after")
                   .orderBy("order_id"))
        changed_rows = changed.collect()
        transitions = {}
        for r in changed_rows:
            k = f"{r['status_before']} -> {r['status_after']}"
            transitions[k] = transitions.get(k, 0) + 1
        print_table(f"Don doi trang thai giua version {v_day1} (sau ngay 1) va {v_day2} (sau ngay 2), 5 dong dau",
                    ["order_id", "truoc", "sau", "updated_at truoc", "updated_at sau"],
                    [[r["order_id"], r["status_before"], r["status_after"], r["updated_before"],
                      r["updated_after"]] for r in changed_rows[:5]])
        print_table("So don theo chuyen trang thai", ["chuyen", "so don"], sorted(transitions.items()))

        # 3. ghi lo sai roi RESTORE
        from delta.tables import DeltaTable
        table = DeltaTable.forPath(spark, path)
        good_version = history[-1]["version"]
        good_rows = sorted_rows(spark.read.format("delta").load(path))
        table.update(condition="order_date = DATE'2026-10-21'", set={"status": "'cancelled'",
                                                                     "total_amount": "0"})
        bad_count = spark.read.format("delta").load(path).where("status = 'cancelled' AND total_amount = 0").count()
        print(f"\nDa co y ghi lo sai: {bad_count} don ngay 2026-10-21 bi doi thanh cancelled, total_amount = 0")
        table.restoreToVersion(good_version)
        restored_rows = sorted_rows(spark.read.format("delta").load(path))
        history_after = history_rows(spark, path)
        print_table("DESCRIBE HISTORY silver.orders sau RESTORE (3 dong cuoi)",
                    ["version", "operation", "metrics"],
                    [[h["version"], h["operation"], h["metrics"]] for h in history_after[-3:]])

        t1_day2 = ws.key["days"][cfg["dates"][1]]["error_counts"]["T1_status_update_resend"]
        print("\nKiem tra:")
        check(len(changed_rows) == t1_day2,
              f"{len(changed_rows)} don doi trang thai sau ngay 2 = so don T1 trong answer key ({t1_day2})")
        check(set(transitions) <= {"pending -> shipping", "shipping -> delivered"},
              f"trang thai chi tien len: {transitions}")
        check(all(r["updated_after"] > r["updated_before"] for r in changed_rows),
              "ban moi co updated_at moi hon")
        check(bad_count > 0, "lo sai da ghi vao bang")
        check(restored_rows == good_rows, f"RESTORE ve version {good_version}: bang khop lai hoan toan")
        check(history_after[-1]["operation"] == "RESTORE", "lich su ghi nhan thao tac RESTORE")
        return {"history": history, "history_after_restore": history_after,
                "version_after_day1": v_day1, "version_after_day2": v_day2,
                "n_status_changed": len(changed_rows), "transitions": transitions,
                "bad_rows": bad_count, "restored_to": good_version}
    finally:
        ws.cleanup()


if __name__ == "__main__":
    run(keep="--keep" in sys.argv)
    print("\nThi nghiem 3: PASS")
