"""Thi nghiem 1 - Tinh luy dang: chay ngay 2026-10-19 hai lan lien tiep.

Ky vong: so dong Bronze, Silver, Gold va doanh thu khong doi; lan chay thu hai
bo qua toan bo tep (khong nap lai), Silver/Gold khong co lo moi.
Chay: python experiments/exp1_idempotency.py [--keep]
"""

import json
import sys

from _helpers import Workspace, check, gold_revenue_by_date, layer_counts, print_table

import run_pipeline  # noqa: E402
from common.run_log import read_ingested_files, read_run_log  # noqa: E402
from common.spark import get_spark  # noqa: E402

DATE = "2026-10-19"


def run(spark=None, keep=False):
    """Chay thi nghiem; tra ve dict ket qua (dung cho reading_numbers.md)."""
    spark = spark or get_spark()
    ws = Workspace("exp1", keep=keep)
    try:
        cfg = ws.cfg("v2")
        snapshots = []
        for attempt in (1, 2):
            run_pipeline.run_date(spark, cfg, DATE, "all")
            snapshots.append({"counts": layer_counts(spark, cfg),
                              "revenue": gold_revenue_by_date(spark, cfg),
                              "ingested_files": read_ingested_files(spark, cfg).count()})
        logs = read_run_log(spark, cfg).orderBy("run_started_at").collect()
        first, second = snapshots

        print_table("So dong moi bang sau lan 1 va lan 2", ["bang", "lan 1", "lan 2"],
                    [[t, first["counts"][t], second["counts"][t]] for t in first["counts"]])
        print_table("Run log", ["layer", "_batch_id", "status", "input_files", "n_input"],
                    [[r["layer"], r["_batch_id"], r["status"], r["input_files"], r["n_input"]]
                     for r in logs])

        bronze_logs = [r for r in logs if r["layer"] == "bronze"]
        second_bronze = bronze_logs[-1]
        print("\nKiem tra:")
        check(first["counts"] == second["counts"], "so dong moi bang Bronze/Silver/Gold khong doi")
        check(first["revenue"] == second["revenue"], f"doanh thu khong doi: {first['revenue']}")
        check(first["ingested_files"] == second["ingested_files"] == 6,
              "ingested_files van 6 tep (khong nap lai)")
        check(second_bronze["_batch_id"] is None and json.loads(second_bronze["input_files"]) == [],
              "lan chay thu hai bo qua toan bo tep")
        second_run = logs[len(logs) // 2:]
        check(all(r["_batch_id"] is None for r in second_run),
              "lan 2: Silver va Gold khong co lo moi de xu ly")
        return {"counts_run1": first["counts"], "counts_run2": second["counts"],
                "revenue_run1": first["revenue"], "revenue_run2": second["revenue"],
                "second_run_input_files": json.loads(second_bronze["input_files"]),
                "run_log": [{"layer": r["layer"], "_batch_id": r["_batch_id"], "status": r["status"],
                             "n_input": r["n_input"]} for r in logs]}
    finally:
        ws.cleanup()


if __name__ == "__main__":
    run(keep="--keep" in sys.argv)
    print("\nThi nghiem 1: PASS")
