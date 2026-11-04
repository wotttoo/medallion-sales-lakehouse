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
from common.run_log import read_ingested_files, utc_now, write_run_log
from common.spark import get_spark
from silver_transform import all_batches, drop_derived_layers, pending_batches, process_batch

LAYERS = ["bronze", "silver"]


class PipelineError(Exception):
    """Loi khi chay mot layer (da ghi run log failed)."""


def parse_args(argv=None):
    """Doc tham so dong lenh."""
    parser = argparse.ArgumentParser(description="Chay pipeline Medallion")
    parser.add_argument("--processing-date", help="ngay xu ly YYYY-MM-DD (trong config.dates)")
    parser.add_argument("--layer", default="all", choices=LAYERS + ["all"])
    parser.add_argument("--rules-version", choices=["v1", "v2"],
                        help="ghi de rules_version trong config")
    parser.add_argument("--rebuild", action="store_true",
                        help="xoa Silver/quarantine/Gold va dung lai tu Bronze")
    parser.add_argument("--config", help="duong dan config.yaml khac")
    parser.add_argument("--lakehouse-dir", help="ghi de paths.lakehouse")
    parser.add_argument("--landing-dir", help="ghi de paths.landing")
    args = parser.parse_args(argv)
    if not args.processing_date and not args.rebuild:
        parser.error("can --processing-date hoac --rebuild")
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


def batch_date(spark, cfg, batch_id):
    """Ngay cua tep nguon trong lo (lay tu ingested_files)."""
    rows = (read_ingested_files(spark, cfg).where(f"_batch_id = '{batch_id}'")
            .select("file_date").distinct().collect())
    return rows[0]["file_date"] if rows else None


def run_silver(spark, cfg, processing_date, batches=None):
    """Xu ly cac lo Bronze chua qua Silver (hoac danh sach batches), moi lo mot dong run log."""
    batches = pending_batches(spark, cfg) if batches is None else batches
    if not batches:
        print(f"[silver] {processing_date}: khong co lo moi")
        return [logged_run(spark, cfg, "silver", processing_date,
                           lambda: {"input_files": [], "n_input": 0})]
    records = []
    for batch_id in batches:
        def work(batch_id=batch_id):
            result = process_batch(spark, cfg, batch_id)
            print(f"[silver] lo {batch_id} ({cfg['rules_version']}): " + ", ".join(
                f"{e} {s['n_valid']} hop le/{s['n_quarantined']} cach ly/"
                f"{s['n_duplicates_removed']} trung" for e, s in result["entities"].items()))
            return {k: result[k] for k in ("_batch_id", "input_files", "n_input", "n_valid",
                                            "n_quarantined", "n_duplicates_removed",
                                            "n_warn_by_rule", "delta_versions_written")}
        records.append(logged_run(spark, cfg, "silver",
                                  batch_date(spark, cfg, batch_id) or processing_date, work))
    return records


def run_rebuild(spark, cfg):
    """--rebuild: xoa Silver, quarantine, Gold roi xu ly lai MOI lo tu Bronze theo thu tu."""
    removed = drop_derived_layers(cfg)
    batches = all_batches(spark, cfg)
    print(f"[rebuild] xoa {removed}; xu ly lai {len(batches)} lo voi {cfg['rules_version']}")
    run_silver(spark, cfg, None, batches=batches)


def run_date(spark, cfg, processing_date, layer):
    """Chay cac layer duoc chon cho mot ngay."""
    if processing_date not in cfg["dates"]:
        raise PipelineError(f"Ngay {processing_date} khong co trong config.dates")
    if layer in ("bronze", "all"):
        run_bronze(spark, cfg, processing_date)
    if layer in ("silver", "all"):
        run_silver(spark, cfg, processing_date)


def main(argv=None, spark=None):
    """Diem vao; tra ve exit code (0 = thanh cong)."""
    args = parse_args(argv)
    cfg = load_config(args.config, lakehouse_dir=args.lakehouse_dir,
                      landing_dir=args.landing_dir, rules_version=args.rules_version)
    spark = spark or get_spark()
    try:
        if args.rebuild:
            run_rebuild(spark, cfg)
        else:
            run_date(spark, cfg, args.processing_date, args.layer)
    except Exception as exc:
        traceback.print_exc()
        print(f"PIPELINE FAILED: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
