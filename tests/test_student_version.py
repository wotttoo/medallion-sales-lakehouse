"""Test sinh ban hoc vien (khong can Spark)."""

import compileall
import subprocess
import sys

import pytest

from common.config import REPO_ROOT

sys.path.insert(0, str(REPO_ROOT / "tools"))
import make_student_version as msv  # noqa: E402

CORE_FILES = ["silver_transform.py", "gold_build.py", "quality_rules.py", "quality_checks.py"]


@pytest.fixture(scope="module")
def student(tmp_path_factory):
    """Sinh ban hoc vien vao thu muc tam."""
    out = tmp_path_factory.mktemp("student")
    replaced = msv.make_student_version(out)
    return out, replaced


def test_no_markers_left(student):
    """Khong con marker BEGIN/END SOLUTION trong ban hoc vien."""
    out, _ = student
    for p in out.rglob("*.py"):
        text = p.read_text(encoding="utf-8")
        assert msv.BEGIN not in text and msv.END not in text, p


def test_compileall_succeeds(student):
    """python -m compileall student thanh cong."""
    out, _ = student
    assert compileall.compile_dir(str(out), quiet=1)
    result = subprocess.run([sys.executable, "-m", "compileall", "-q", str(out)],
                            capture_output=True, text=True)
    assert result.returncode == 0, result.stdout + result.stderr


def test_core_blocks_replaced_with_todo(student):
    """Moi tep cot loi co TODO + NotImplementedError, dung so khoi trong ban solution."""
    out, replaced = student
    for name in CORE_FILES:
        n_solution = (REPO_ROOT / name).read_text(encoding="utf-8").count(msv.BEGIN)
        assert n_solution >= 1
        assert replaced[name] == n_solution
        text = (out / name).read_text(encoding="utf-8")
        assert text.count("raise NotImplementedError") >= n_solution
        assert "# TODO: hoan thanh" in text and "Goi y:" in text


def test_other_files_copied_unchanged(student):
    """Tep khong co marker (generator, bronze, test, config) giu nguyen."""
    out, _ = student
    for rel in ["generate_data.py", "bronze_ingest.py", "config.yaml", "tests/test_against_answer_key.py",
                "common/spark.py", "README.md"]:
        assert (out / rel).read_bytes() == (REPO_ROOT / rel).read_bytes(), rel
    assert not (out / "PLAN.md").exists() and not (out / "tools" / "make_student_version.py").exists()


def test_student_modules_import_and_raise(student):
    """Module hoc vien import duoc; goi ham TODO thi NotImplementedError."""
    out, _ = student
    code = ("import bronze_ingest, silver_transform, gold_build, quality_checks, run_pipeline\n"
            "import quality_rules\n"
            "try:\n    quality_rules.all_rules()\nexcept NotImplementedError:\n    print('TODO_OK')\n")
    result = subprocess.run([sys.executable, "-c", code], cwd=str(out), capture_output=True, text=True)
    assert result.returncode == 0, result.stderr
    assert "TODO_OK" in result.stdout


def test_bad_markers_rejected():
    """Thieu END hoac long nhau thi bao loi ro rang."""
    with pytest.raises(ValueError, match="thieu END"):
        msv.replace_solution_blocks("def f():\n    # BEGIN SOLUTION\n    return 1\n", "x.py")
    with pytest.raises(ValueError, match="khong co BEGIN"):
        msv.replace_solution_blocks("def f():\n    # END SOLUTION\n", "x.py")
    text, n = msv.replace_solution_blocks(
        "def f():\n    a = 1\n    # BEGIN SOLUTION\n    return a\n    # END SOLUTION\n", "x.py")
    assert n == 1 and "raise NotImplementedError" in text and "return a" not in text
