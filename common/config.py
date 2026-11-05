"""Doc config.yaml va cho phep ghi de mot so tham so (dung cho test, thi nghiem)."""

import copy
from pathlib import Path

import yaml

# thu muc goc cua repo (chua config.yaml)
REPO_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_CONFIG_PATH = REPO_ROOT / "config.yaml"


def load_config(config_path=None, lakehouse_dir=None, landing_dir=None,
                answer_key_path=None, rules_version=None):
    """Doc config.yaml, ghi de duong dan / rules_version neu duoc truyen vao.

    Duong dan tuong doi trong config duoc hieu la tuong doi voi thu muc goc repo,
    nen chay tu thu muc nao cung ra cung ket qua.
    """
    path = Path(config_path) if config_path else DEFAULT_CONFIG_PATH
    with path.open("r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f)

    cfg = copy.deepcopy(cfg)
    if lakehouse_dir is not None:
        cfg["paths"]["lakehouse"] = str(lakehouse_dir)
    if landing_dir is not None:
        cfg["paths"]["landing"] = str(landing_dir)
    if answer_key_path is not None:
        cfg["paths"]["answer_key"] = str(answer_key_path)
    if rules_version is not None:
        cfg["rules_version"] = rules_version

    if cfg["rules_version"] not in ("v1", "v2"):
        raise ValueError(f"rules_version khong hop le: {cfg['rules_version']}")
    return cfg


def resolve_path(cfg, key):
    """Tra ve duong dan tuyet doi (Path) cho mot muc trong cfg['paths']."""
    p = Path(cfg["paths"][key])
    if not p.is_absolute():
        p = REPO_ROOT / p
    return p
