"""Carga de la configuración y de los secretos.

La configuración sale de config/config.yaml si existe (copia local, ignorada por git) o, si no,
de config/config.example.yaml. Los secretos nunca están en esos archivos: se leen de variables de entorno.
"""
import os
from dataclasses import dataclass
from pathlib import Path

import yaml

# src/cpa/config.py -> la raíz del repo está dos niveles arriba de src/.
REPO_ROOT = Path(__file__).resolve().parents[2]

ZONES = ("landing", "bronze", "silver", "gold", "quarantine", "checkpoints", "meta")


@dataclass
class Config:
    raw: dict            # el YAML completo, para leer parámetros de cada etapa
    source: Path         # qué archivo se cargó (queda en el run_log)
    lake_root: Path
    paths: dict          # zona -> ruta absoluta (str), lista para Spark
    evidence_dir: Path   # dónde se guardan las evidencias de la entrega

    def get(self, section, key, default=None):
        return self.raw.get(section, {}).get(key, default)


def _resolve(path, base):
    """Ruta absoluta: si es relativa, se resuelve contra `base`."""
    p = Path(path).expanduser()
    return p if p.is_absolute() else (base / p).resolve()


def load_config(path=None):
    """Lee el YAML y resuelve las rutas del lago.

    Orden de prioridad del archivo: argumento `path`, variable CPA_CONFIG, config/config.yaml,
    config/config.example.yaml. La raíz del lago se puede pisar con CPA_LAKE_ROOT y la carpeta de
    evidencia con CPA_EVIDENCE_DIR.
    """
    candidates = [path, os.environ.get("CPA_CONFIG"),
                  REPO_ROOT / "config" / "config.yaml", REPO_ROOT / "config" / "config.example.yaml"]
    source = next(Path(c) for c in candidates if c and Path(c).exists())
    with open(source, encoding="utf-8") as fh:
        raw = yaml.safe_load(fh)

    lake = raw["lake"]
    lake_root = _resolve(os.environ.get("CPA_LAKE_ROOT", lake["root"]), REPO_ROOT)
    paths = {zone: str(_resolve(lake[zone], lake_root)) for zone in ZONES}
    evidence_dir = _resolve(os.environ.get("CPA_EVIDENCE_DIR", raw["evidence_dir"]), REPO_ROOT)
    return Config(raw=raw, source=source, lake_root=lake_root, paths=paths, evidence_dir=evidence_dir)


def astra_secrets():
    """Token y bundle de AstraDB desde variables de entorno (en Colab se copian desde Colab Secrets).

    Se piden recién al cargar en AstraDB, así el resto del pipeline corre sin credenciales.
    """
    token = os.environ.get("ASTRA_DB_TOKEN")
    bundle = os.environ.get("ASTRA_DB_BUNDLE_PATH")
    if not token or not bundle:
        raise RuntimeError("Faltan ASTRA_DB_TOKEN o ASTRA_DB_BUNDLE_PATH (ver config/config.example.yaml)")
    if not Path(bundle).exists():
        raise FileNotFoundError(f"No existe el secure connect bundle: {bundle}")
    return token, bundle
