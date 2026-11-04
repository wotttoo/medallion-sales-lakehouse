"""Dieu phoi pipeline Bronze -> Silver -> Gold.

Vi du:
    python run_pipeline.py --processing-date 2026-10-19 --layer all
    python run_pipeline.py --processing-date 2026-10-19 --layer bronze
Moi layer chay xong ghi mot dong run log; loi thi ghi status = failed va exit code 1.
"""

import argparse
import sys
import traceback

from bronze_ingest import ingest_date
from common.config import load_config
from common.run_log import utc_now, write_run_log
from common.spark import get_spark

LAYERS = ["bronze"]


class PipelineError(Exception):
    """Loi khi chay mot layer (da ghi run log failed)."""


def parse_args(argv=None):
    """Doc tham so dong lenh."""
    parser = argparse.ArgumentParser(description="Chay pipeline Medallion")
    parser.add_argument("--processing-date", help="ngay xu ly YYYY-MM-DD (trong config.dates)")
    parser.add_argument("--layer", default="all", choices=LAYERS + ["all"])
    parser.add_argument("--rules-version", choices=["v1", "v2"],
                        help="ghi de rules_version trong config")
    parser.add_argument("--config", help="duong dan config.yaml khac")
    parser.add_argument("--lakehouse-dir", help="ghi de paths.lakehouse")
    parser.add_argument("--landing-dir", help="ghi de paths.landing")
    args = parser.parse_args(argv)
    if not args.processing_date:
        parser.error("can --processing-date")
    return args


def logged_run(spark, cfg, layer, processing_date, fn):
    """Chay fn() va ghi mot dong run log; fn tra ve dict cac cot run log.

    Loi thi ghi status = failed roi nem PipelineError.
    """
    started = utc_now()
    record = {"layer": layer, "processing_date": processing_date,
              "rules_version": cfg["rules_version"], "run_started_at": started}
    try:
        record.update(fn() or {})
        record["status"] = "success"
    except Exception as exc:
        record["status"] = "failed"
        record["run_finished_at"] = utc_now()
        write_run_log(spark, cfg, record)
        raise PipelineError(f"Layer {layer} ngay {processing_date} that bai: {exc}") from exc
    record["run_finished_at"] = utc_now()
    write_run_log(spark, cfg, record)
    return record


def run_bronze(spark, cfg, processing_date):
    """Nap tep landing cua ngay vao Bronze, ghi run log."""
    def work():
        result = ingest_date(spark, cfg, processing_date)
        print(f"[bronze] {processing_date}: lo {result['_batch_id']}, "
              f"nap {len(result['input_files'])} tep, bo qua {len(result['skipped_files'])} tep")
        return {"_batch_id": result["_batch_id"], "input_files": result["input_files"],
                "n_input": result["n_input"],
                "delta_versions_written": result["delta_versions_written"]}
    return logged_run(spark, cfg, "bronze", processing_date, work)


def run_date(spark, cfg, processing_date, layer):
    """Chay cac layer duoc chon cho mot ngay."""
    if processing_date not in cfg["dates"]:
        raise PipelineError(f"Ngay {processing_date} khong co trong config.dates")
    if layer in ("bronze", "all"):
        run_bronze(spark, cfg, processing_date)


def main(argv=None, spark=None):
    """Diem vao; tra ve exit code (0 = thanh cong)."""
    args = parse_args(argv)
    cfg = load_config(args.config, lakehouse_dir=args.lakehouse_dir,
                      landing_dir=args.landing_dir, rules_version=args.rules_version)
    spark = spark or get_spark()
    try:
        run_date(spark, cfg, args.processing_date, args.layer)
    except Exception as exc:
        traceback.print_exc()
        print(f"PIPELINE FAILED: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
