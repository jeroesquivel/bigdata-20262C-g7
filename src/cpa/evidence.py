"""Evidencia de la entrega: archivos con el run_id en el nombre que nunca se pisan.

Las rutas que se guardan son relativas a la raíz del repo, para que la evidencia no dependa de la máquina.
"""
import json
from pathlib import Path

from cpa.config import REPO_ROOT


def rel(path):
    """Ruta relativa al repo si está adentro; si no, la ruta tal cual."""
    p = Path(str(path).removeprefix("file:")).resolve()
    return str(p.relative_to(REPO_ROOT)) if p.is_relative_to(REPO_ROOT) else str(path)


def _new_path(cfg, name, run_id, ext):
    cfg.evidence_dir.mkdir(parents=True, exist_ok=True)
    path = cfg.evidence_dir / f"{name}_{run_id}.{ext}"
    n = 1
    while path.exists():
        path = cfg.evidence_dir / f"{name}_{run_id}_{n}.{ext}"
        n += 1
    return path


def save_json(cfg, name, run_id, obj):
    path = _new_path(cfg, name, run_id, "json")
    path.write_text(json.dumps(obj, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    return path


def save_text(cfg, name, run_id, text, ext="txt"):
    path = _new_path(cfg, name, run_id, ext)
    path.write_text(text, encoding="utf-8")
    return path


def markdown_table(rows):
    """Lista de dicts -> tabla Markdown (para los .md de evidencia)."""
    if not rows:
        return "(sin filas)\n"
    cols = list(rows[0].keys())
    lines = ["| " + " | ".join(cols) + " |", "|" + "---|" * len(cols)]
    lines += ["| " + " | ".join("" if r.get(c) is None else str(r.get(c)) for c in cols) + " |" for r in rows]
    return "\n".join(lines) + "\n"


def explain_text(df, mode="formatted"):
    """Plan físico de Spark como texto (df.explain imprime con print, así que se captura la salida)."""
    import contextlib
    import io
    buf = io.StringIO()
    with contextlib.redirect_stdout(buf):
        df.explain(mode=mode)
    return hide_local_paths(buf.getvalue())


def hide_local_paths(text):
    """Reemplaza la ruta absoluta del repo por <repo> (los planes de Spark muestran la ruta de cada lectura)."""
    from urllib.parse import quote
    for root in (str(REPO_ROOT), quote(str(REPO_ROOT))):
        text = text.replace(root, "<repo>")
    return text.replace(str(Path.home()), "~")
