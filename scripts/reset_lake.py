"""Borra las zonas generadas del lago (Bronze, Silver, Gold, quarantine y checkpoints) para volver a correr
el pipeline desde cero. Landing no se toca nunca; _meta (historial de corridas) se conserva salvo --incluir-meta.

Uso:  python scripts/reset_lake.py [--incluir-meta] [--si] [--config RUTA]
"""
import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from cpa.config import load_config  # noqa: E402
from cpa.lake import reset_lake  # noqa: E402


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--incluir-meta", action="store_true", help="también borra _meta (run_log)")
    parser.add_argument("--si", action="store_true", help="no pedir confirmación")
    parser.add_argument("--config", help="otro YAML (por ejemplo tests/fixtures/config_fixture.yaml)")
    args = parser.parse_args()

    cfg = load_config(args.config)
    print(f"Lago: {cfg.lake_root}")
    if not args.si and input("¿Borrar las zonas generadas? [s/N] ").strip().lower() != "s":
        print("Cancelado.")
        return
    for path in reset_lake(cfg, include_meta=args.incluir_meta):
        print("borrado:", path)


if __name__ == "__main__":
    main()
