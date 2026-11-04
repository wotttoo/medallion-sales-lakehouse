"""Nen landing/ thanh data_landing.zip de phat cho hoc vien.

Zip tat dinh: thu tu tep co dinh, moc thoi gian co dinh, duong dan dang posix,
nen cung du lieu luon cho cung mot tep zip.
Chay: python tools/package_data.py [--landing-dir landing] [--output data_landing.zip]
"""

import argparse
import sys
import zipfile
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from common.config import load_config, resolve_path  # noqa: E402

FIXED_TIME = (2026, 10, 19, 0, 0, 0)


def package_landing(landing_dir, output):
    """Nen moi tep trong landing_dir vao output (goc trong zip la 'landing/'); tra ve so tep."""
    landing_dir, output = Path(landing_dir), Path(output)
    files = sorted(p for p in landing_dir.rglob("*") if p.is_file())
    if not files:
        raise FileNotFoundError(f"Khong co tep trong {landing_dir}; chay python generate_data.py truoc")
    with zipfile.ZipFile(output, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        for p in files:
            info = zipfile.ZipInfo("landing/" + p.relative_to(landing_dir).as_posix(), FIXED_TIME)
            info.compress_type = zipfile.ZIP_DEFLATED
            info.external_attr = 0o644 << 16
            zf.writestr(info, p.read_bytes())
    return len(files)


def main(argv=None):
    """Diem vao dong lenh."""
    cfg = load_config()
    parser = argparse.ArgumentParser(description="Nen landing/ thanh data_landing.zip")
    parser.add_argument("--landing-dir", default=str(resolve_path(cfg, "landing")))
    parser.add_argument("--output", default=str(REPO_ROOT / "data_landing.zip"))
    args = parser.parse_args(argv)
    n = package_landing(args.landing_dir, args.output)
    print(f"Da nen {n} tep vao {args.output}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
