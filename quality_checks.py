"""Kiem tra muc tap du lieu (SPEC 7.5). Moi check vi pham thi raise QualityCheckError.

Check sau layer:
- bronze: volume_check
- silver: record_conservation, quarantine_rate
- gold:   revenue_reconciliation, no_orphan_keys
"""


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
