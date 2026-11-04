"""Kiem tra muc tap du lieu (SPEC 7.5). Moi check vi pham thi raise QualityCheckError.

Check sau layer:
- bronze: volume_check
- silver: record_conservation, quarantine_rate
- gold:   revenue_reconciliation, no_orphan_keys
"""

from common.run_log import read_ingested_files


class QualityCheckError(Exception):
    """Mot check muc tap du lieu that bai (pipeline phai fail)."""


# ---------------------------------------------------------------------------
# record_conservation
# ---------------------------------------------------------------------------
def check_record_conservation(entity_stats):
    """n_input = n_valid + n_quarantined + n_duplicates_removed cho moi thuc the cua lo.

    entity_stats: dict entity -> dict cac so dem (tu silver_transform.process_batch).
    """
    # BEGIN SOLUTION
    broken = {}
    for entity, s in entity_stats.items():
        total = s["n_valid"] + s["n_quarantined"] + s["n_duplicates_removed"]
        if s["n_input"] != total:
            broken[entity] = f"n_input={s['n_input']} != {total}"
    # END SOLUTION
    if broken:
        raise QualityCheckError(f"record_conservation that bai: {broken}")
    return {"record_conservation": "ok"}


# ---------------------------------------------------------------------------
# quarantine_rate
# ---------------------------------------------------------------------------
def check_quarantine_rate(entity_stats, threshold):
    """Ty le cach ly cua lo (moi thuc the) phai < threshold; cham nguong thi fail."""
    # BEGIN SOLUTION
    rates, broken = {}, {}
    for entity, s in entity_stats.items():
        if s["n_input"] == 0:
            continue
        rate = s["n_quarantined"] / s["n_input"]
        rates[entity] = round(rate, 4)
        if rate >= threshold:
            broken[entity] = round(rate, 4)
    # END SOLUTION
    if broken:
        raise QualityCheckError(f"quarantine_rate >= {threshold}: {broken}")
    return {"quarantine_rate": rates}


# ---------------------------------------------------------------------------
# volume_check
# ---------------------------------------------------------------------------
def daily_order_counts(spark, cfg):
    """So dong orders da nap theo ngay tep (tu _meta/ingested_files)."""
    rows = (read_ingested_files(spark, cfg).where("entity = 'orders'")
            .groupBy("file_date").sum("row_count").collect())
    return {r["file_date"]: int(r[1]) for r in rows}


def check_volume_counts(processing_date, counts, threshold):
    """So don ngay xu ly >= threshold x trung binh cac ngay truoc; bo qua ngay dau."""
    previous = [n for d, n in counts.items() if d < processing_date]
    if not previous:
        return {"volume_check": "skipped (ngay dau)"}
    current = counts.get(processing_date, 0)
    average = sum(previous) / len(previous)
    if current < threshold * average:
        raise QualityCheckError(
            f"volume_check that bai: {processing_date} co {current} don < "
            f"{threshold} x trung binh {average:.1f}")
    return {"volume_check": {"current": current, "previous_average": round(average, 1)}}


def check_volume(spark, cfg, processing_date):
    """volume_check cho ngay xu ly, doc so dong tu ingested_files."""
    return check_volume_counts(processing_date, daily_order_counts(spark, cfg),
                               cfg["volume_drop_threshold"])
