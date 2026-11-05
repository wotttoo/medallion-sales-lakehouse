"""Xuat reading_numbers.md: cac con so that de tac gia dien vao muc Thuc hanh cua reading.

Gom: so dong moi lop moi ngay (v1, v2), so cach ly theo quy tac, ban ghi minh hoa,
ket qua 3 thi nghiem va trich DESCRIBE HISTORY.
Chay: python tools/export_reading_numbers.py [--output reading_numbers.md]
(mat khoang 10-15 phut vi chay pipeline nhieu lan tren workspace tam).
"""

import argparse
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
for p in (REPO_ROOT, REPO_ROOT / "experiments"):
    if str(p) not in sys.path:
        sys.path.insert(0, str(p))

import exp1_idempotency  # noqa: E402
import exp2_rule_change  # noqa: E402
import exp3_time_travel  # noqa: E402
import run_pipeline  # noqa: E402
from _helpers import ENTITIES, Workspace, layer_counts  # noqa: E402
from bronze_ingest import read_bronze  # noqa: E402
from common.run_log import read_run_log  # noqa: E402
from common.spark import get_spark  # noqa: E402
from silver_transform import read_quarantine, read_silver  # noqa: E402


def md_table(headers, rows):
    """Bang Markdown."""
    out = ["| " + " | ".join(headers) + " |", "|" + "---|" * len(headers)]
    out += ["| " + " | ".join(str(c) for c in r) + " |" for r in rows]
    return "\n".join(out)


def quarantine_by_rule(spark, cfg):
    """So ban ghi cach ly vi pham tung quy tac (mot ban ghi co the dem nhieu quy tac)."""
    counts = {}
    for entity in ENTITIES:
        for r in read_quarantine(spark, cfg, entity).select("_failed_rules").collect():
            for rule in r["_failed_rules"].split("; "):
                counts[(entity, rule)] = counts.get((entity, rule), 0) + 1
    return counts


def run_version(spark, ws, version):
    """Chay 3 ngay voi mot version; tra ve so lieu tung ngay va tong hop cuoi."""
    cfg = ws.cfg(version)
    per_day = {}
    for date in cfg["dates"]:
        run_pipeline.run_date(spark, cfg, date, "all")
        per_day[date] = layer_counts(spark, cfg)
    silver_logs = {r["_batch_id"]: r for r in
                   read_run_log(spark, cfg).where("layer = 'silver' AND status = 'success'").collect()}
    return {"cfg": cfg, "per_day": per_day, "silver_logs": silver_logs,
            "q_by_rule": quarantine_by_rule(spark, cfg)}


def showcase_section(spark, cfg_v1):
    """Ba dong Bronze minh hoa, ORD-1001 o Silver, ORD-1002 o quarantine (v1)."""
    bronze = (read_bronze(spark, cfg_v1, "orders").where("order_id IN ('ORD-1001', 'ORD-1002')")
              .orderBy("_ingest_time").collect())
    cols = ["order_id", "customer_id", "order_date", "total_amount", "status", "_source_file",
            "_batch_id", "_ingest_time"]
    lines = ["### Bronze `bronze.orders_raw`", "",
             md_table(cols, [[r[c] if r[c] is not None else "" for c in cols] for r in bronze]), ""]
    s = read_silver(spark, cfg_v1, "orders").where("order_id = 'ORD-1001'").collect()[0]
    cols = ["order_id", "customer_id", "order_date", "total_amount", "status", "channel", "updated_at"]
    lines += ["### Silver `silver.orders` (ORD-1001 con mot dong)", "",
              md_table(cols, [[s[c] for c in cols]]), ""]
    q = read_quarantine(spark, cfg_v1, "orders").where("order_id = 'ORD-1002'").collect()[0]
    cols = ["order_id", "customer_id", "order_date", "total_amount", "status", "_failed_rules", "_batch_id"]
    lines += ["### Quarantine `silver.orders_quarantine` (v1)", "",
              md_table(cols, [[q[c] if q[c] is not None else "" for c in cols]]), ""]
    return lines


def build_markdown(runs, exp1, exp2, exp3):
    """Ghep noi dung reading_numbers.md."""
    v1, v2 = runs["v1"], runs["v2"]
    dates = v1["cfg"]["dates"]
    lines = ["# Con so that cho muc Thuc hanh", "",
             "File nay do `python tools/export_reading_numbers.py` sinh ra tu mot lan chay that "
             "(seed 42, du lieu trong repo). Chay lai se ra cung so dong/so cach ly/doanh thu; "
             "chi timestamp va `_ingest_time` khac.", ""]

    lines += ["## 1. So dong moi lop sau tung ngay (rules_version = v2)", ""]
    tables = list(v2["per_day"][dates[0]])
    lines += [md_table(["bang"] + dates, [[t] + [v2["per_day"][d][t] for d in dates] for t in tables]), ""]

    lines += ["## 2. Silver tung lo: hop le / cach ly / trung bi loai", ""]
    for version, run in (("v1", v1), ("v2", v2)):
        rows = []
        for i, d in enumerate(dates, start=1):
            log = run["silver_logs"][f"b-{i:04d}"]
            rows.append([d, f"b-{i:04d}", log["n_input"], log["n_valid"], log["n_quarantined"],
                         log["n_duplicates_removed"], log["n_warn_by_rule"]])
        lines += [f"### {version}", "", md_table(
            ["ngay", "_batch_id", "n_input", "n_valid", "n_quarantined", "n_duplicates_removed",
             "n_warn_by_rule"], rows), ""]

    lines += ["## 3. So ban ghi cach ly theo quy tac (tong 3 ngay)", "",
              "Mot ban ghi vi pham nhieu quy tac duoc dem o moi quy tac.", ""]
    keys = sorted(set(v1["q_by_rule"]) | set(v2["q_by_rule"]))
    lines += [md_table(["thuc the", "quy tac", "v1", "v2"],
                       [[e, r, v1["q_by_rule"].get((e, r), 0), v2["q_by_rule"].get((e, r), 0)]
                        for e, r in keys]), ""]

    lines += ["## 4. Ban ghi minh hoa", ""] + runs["showcase"]

    lines += ["## 5. Thi nghiem 1 - Luy dang (chay 2026-10-19 hai lan)", ""]
    lines += [md_table(["bang", "lan 1", "lan 2"],
                       [[t, exp1["counts_run1"][t], exp1["counts_run2"][t]] for t in exp1["counts_run1"]]), ""]
    lines += [f"- Doanh thu lan 1: `{exp1['revenue_run1']}`, lan 2: `{exp1['revenue_run2']}`",
              f"- Tep nap o lan 2: `{exp1['second_run_input_files']}` (bo qua toan bo 6 tep)", ""]

    lines += ["## 6. Thi nghiem 2 - Doi quy tac v1 -> v2 (--rebuild)", ""]
    lines += [md_table(["thuc the", "cach ly v1", "cach ly v2"],
                       [[e, exp2["v1"]["n_quarantined"][e], exp2["v2"]["n_quarantined"][e]] for e in ENTITIES]), ""]
    rev1, rev2 = exp2["v1"]["revenue_by_date"], exp2["v2"]["revenue_by_date"]
    lines += [md_table(["order_date", "doanh thu v1", "doanh thu v2", "tang"],
                       [[d, rev1[d], rev2[d], rev2[d] - rev1[d]] for d in rev1]), ""]
    lines += [f"- So don cach ly giam: **{exp2['quarantine_decrease']}** (= so don chi dinh A1/A2)",
              f"- Doanh thu tang: **{exp2['revenue_increase']}** VND",
              f"- `_failed_rules` cua ORD-1002: v1 `{exp2['v1']['ord_1002_rules'][0]}`, "
              f"v2 `{exp2['v2']['ord_1002_rules'][0]}`",
              f"- Bronze khong doi: phien ban `{exp2['v1']['bronze_versions']}`, "
              f"so dong `{exp2['v1']['bronze_rows']}`", ""]

    lines += ["## 7. Thi nghiem 3 - Time travel", "", "### DESCRIBE HISTORY silver.orders", ""]
    lines += [md_table(["version", "timestamp", "operation", "operationMetrics (rut gon)"],
                       [[h["version"], h["timestamp"], h["operation"], h["metrics"]]
                        for h in exp3["history"]]), ""]
    lines += [f"- Phien ban sau ngay 1: `{exp3['version_after_day1']}`, sau ngay 2: "
              f"`{exp3['version_after_day2']}`",
              f"- So don doi trang thai giua hai phien ban (T1): **{exp3['n_status_changed']}** "
              f"`{exp3['transitions']}`",
              f"- Lo sai co y ghi vao: {exp3['bad_rows']} dong; RESTORE ve version "
              f"`{exp3['restored_to']}`, bang khop lai hoan toan", ""]
    lines += ["### DESCRIBE HISTORY sau RESTORE (3 dong cuoi)", "",
              md_table(["version", "operation", "operationMetrics (rut gon)"],
                       [[h["version"], h["operation"], h["metrics"]]
                        for h in exp3["history_after_restore"][-3:]]), ""]
    return "\n".join(lines)


def main(argv=None):
    """Chay cac lan do va ghi reading_numbers.md."""
    parser = argparse.ArgumentParser(description="Xuat reading_numbers.md")
    parser.add_argument("--output", default=str(REPO_ROOT / "reading_numbers.md"))
    args = parser.parse_args(argv)
    spark = get_spark()
    runs = {}
    for version in ("v1", "v2"):
        ws = Workspace(f"numbers_{version}")
        try:
            runs[version] = run_version(spark, ws, version)
            if version == "v1":
                runs["showcase"] = showcase_section(spark, runs["v1"]["cfg"])
        finally:
            ws.cleanup()
    exp1 = exp1_idempotency.run(spark)
    exp2 = exp2_rule_change.run(spark)
    exp3 = exp3_time_travel.run(spark)
    Path(args.output).write_text(build_markdown(runs, exp1, exp2, exp3) + "\n", encoding="utf-8")
    print(f"Da ghi {args.output}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
