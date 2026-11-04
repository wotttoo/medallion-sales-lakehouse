"""Sinh ban cho hoc vien: thay moi khoi BEGIN/END SOLUTION bang TODO + raise NotImplementedError.

Chay: python tools/make_student_version.py [--output-dir student]
Thu muc dau ra la san pham sinh ra (khong commit), duoc tao lai moi lan chay.
"""

import argparse
import py_compile
import re
import shutil
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent

BEGIN = "# BEGIN SOLUTION"
END = "# END SOLUTION"

# tep/thu muc chep sang ban hoc vien
INCLUDE = ["README.md", "SPEC.md", "requirements.txt", "pytest.ini", "config.yaml",
           "generate_data.py", "bronze_ingest.py", "silver_transform.py", "gold_build.py",
           "quality_rules.py", "quality_checks.py", "run_pipeline.py",
           "common", "experiments", "tests", "tools/export_reading_numbers.py"]
SKIP_PARTS = {"__pycache__", ".pytest_cache"}
# test cua chinh cong cu nay (co chuoi marker) khong thuoc ban hoc vien
SKIP_FILES = {"tests/test_student_version.py"}

# goi y ngan (tieng Viet khong dau) theo ten ham chua khoi solution
HINTS = {
    "parse_order_date": "v1 chi YYYY-MM-DD; v2 them DD/MM/YYYY. Dung parse_date() va F.coalesce.",
    "parse_amount": "v1 chi so thuan (PLAIN_NUMBER_PATTERN); v2 them chuoi '150.000đ' "
                    "(VND_STRING_PATTERN): bo ky tu khong phai so roi try_cast('long').",
    "rule_condition": "tra ve bieu thuc SQL (TRUE = dat) cho tung ten quy tac trong RULE_SPECS; "
                      "cot da chuan hoa, vd 'customer_id IS NOT NULL'.",
    "rule_passed": "F.expr(rule['condition']); NULL tinh la vi pham -> F.coalesce(..., F.lit(False)).",
    "failed_rules_column": "moi quy tac drop vi pham -> F.lit(ten); noi bang F.concat_ws('; ', ...); "
                           "khong vi pham quy tac nao -> NULL.",
    "split_valid_quarantine": "_failed_rules NULL -> valid (bo cot _failed_rules); con lai -> quarantined.",
    "deduplicate": "Window.partitionBy(*ENTITY_KEYS[entity]).orderBy(*dedup_order(valid)), "
                   "giu dong co row_number() = 1.",
    "merge_into_silver": "DeltaTable.forPath(...).alias('t').merge(source.alias('s'), dieu kien khoa); "
                         "orders chi update khi s.updated_at >= t.updated_at; whenNotMatchedInsertAll.",
    "affected_dates": "order_id cua dong orders + order_items trong lo Bronze -> tra order_date "
                      "trong silver.orders (gom don den muon, don cap nhat).",
    "replace_dates": "loc df theo dates; lan dau ghi partitionBy('order_date'); sau do mode overwrite "
                     "+ option replaceWhere = date_predicate(dates).",
    "revenue_lines": "doc gold.fact_sales, bo don status = 'cancelled'.",
    "check_record_conservation": "voi moi thuc the: n_input == n_valid + n_quarantined + "
                                 "n_duplicates_removed, sai thi ghi vao broken.",
    "check_quarantine_rate": "rate = n_quarantined / n_input (bo qua n_input = 0); rate >= threshold "
                             "-> ghi vao broken; luu rates.",
    "check_revenue_reconciliation": "so silver_revenue_by_date voi gold_revenue_by_date cua ca hai "
                                    "bang KPI; ngay nao lech thi ghi vao broken.",
    "check_no_orphan_keys": "left_anti join cot khoa cua fact voi bang dim; dem so khoa thieu vao n.",
}


def enclosing_function(lines, index):
    """Ten ham gan nhat phia tren dong index."""
    for line in reversed(lines[:index]):
        m = re.match(r"\s*def\s+(\w+)\s*\(", line)
        if m:
            return m.group(1)
    return None


def replace_solution_blocks(text, rel_path):
    """Thay moi khoi solution bang TODO + raise NotImplementedError; tra ve (text moi, so khoi)."""
    lines = text.splitlines(keepends=True)
    out, i, n_blocks = [], 0, 0
    while i < len(lines):
        line = lines[i]
        if line.strip() == BEGIN:
            indent = line[: len(line) - len(line.lstrip())]
            j = i + 1
            while j < len(lines) and lines[j].strip() != END:
                if lines[j].strip() == BEGIN:
                    raise ValueError(f"{rel_path}:{j + 1}: BEGIN SOLUTION long nhau")
                j += 1
            if j == len(lines):
                raise ValueError(f"{rel_path}:{i + 1}: thieu END SOLUTION")
            func = enclosing_function(lines, i)
            hint = HINTS.get(func, "xem docstring cua ham va SPEC.md")
            out.append(f"{indent}# TODO: hoan thanh {func or 'khoi nay'}. Goi y: {hint}\n")
            out.append(f'{indent}raise NotImplementedError("TODO: {func or rel_path}")\n')
            n_blocks += 1
            i = j + 1
            continue
        if line.strip() == END:
            raise ValueError(f"{rel_path}:{i + 1}: END SOLUTION khong co BEGIN")
        out.append(line)
        i += 1
    return "".join(out), n_blocks


def iter_source_files():
    """Liet ke tep can chep (duong dan tuong doi)."""
    for item in INCLUDE:
        path = REPO_ROOT / item
        if path.is_dir():
            for p in sorted(path.rglob("*")):
                rel = p.relative_to(REPO_ROOT)
                if p.is_file() and not (SKIP_PARTS & set(p.parts)) and rel.as_posix() not in SKIP_FILES:
                    yield rel
        elif path.is_file():
            yield Path(item)


def make_student_version(output_dir):
    """Tao ban hoc vien trong output_dir; tra ve dict tep -> so khoi da thay."""
    output_dir = Path(output_dir)
    if output_dir.exists():
        shutil.rmtree(output_dir)
    replaced = {}
    for rel in iter_source_files():
        src, dst = REPO_ROOT / rel, output_dir / rel
        dst.parent.mkdir(parents=True, exist_ok=True)
        if rel.suffix == ".py":
            text, n = replace_solution_blocks(src.read_text(encoding="utf-8"), rel.as_posix())
            dst.write_text(text, encoding="utf-8", newline="")
            if n:
                replaced[rel.as_posix()] = n
            py_compile.compile(str(dst), doraise=True)
        else:
            shutil.copy2(src, dst)
    return replaced


def main(argv=None):
    """Diem vao dong lenh."""
    parser = argparse.ArgumentParser(description="Sinh ban cho hoc vien")
    parser.add_argument("--output-dir", default=str(REPO_ROOT / "student"))
    args = parser.parse_args(argv)
    replaced = make_student_version(args.output_dir)
    for rel, n in replaced.items():
        print(f"{rel}: thay {n} khoi solution")
    print(f"Da tao ban hoc vien tai {args.output_dir}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
