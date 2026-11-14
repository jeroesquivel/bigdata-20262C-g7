"""Registro de corridas (_meta/run_log): una fila por etapa con conteos, duración y estado.

Es metadato operativo, no dato del negocio, así que se escribe desde Python como JSON Lines:
un archivo por corrida con fecha y hora en el nombre, que nunca se pisa (tampoco el de una corrida fallida).
Spark lo puede leer con spark.read.json(<_meta>/run_log).
"""
import json
import time
import traceback
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path


class RunLog:
    def __init__(self, cfg):
        self.run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        log_dir = Path(cfg.paths["meta"]) / "run_log"
        log_dir.mkdir(parents=True, exist_ok=True)
        path = log_dir / f"run_{self.run_id}.jsonl"
        n = 1
        while path.exists():  # dos corridas en el mismo segundo: se agrega un sufijo, no se pisa
            path = log_dir / f"run_{self.run_id}_{n}.jsonl"
            n += 1
        self.path = path
        # Ruta relativa al repo: el run_log no debe guardar rutas personales de la máquina.
        from cpa.config import REPO_ROOT
        src = Path(cfg.source).resolve()
        self.config_source = str(src.relative_to(REPO_ROOT)) if src.is_relative_to(REPO_ROOT) else src.name
        self.rows = []

    @contextmanager
    def stage(self, name):
        """Mide una etapa y la registra al terminar, haya salido bien o no.

        Uso:
            with run_log.stage("bronze_batch") as st:
                st["rows_read"] = ...
        Las claves estándar son rows_read, rows_written, rows_rejected (quarantine) y rows_duplicated;
        cada etapa puede agregar otras (por ejemplo, detalle por fuente).
        """
        row = {"run_id": self.run_id, "stage": name,
               "started_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
               "rows_read": None, "rows_written": None, "rows_rejected": None, "rows_duplicated": None}
        t0 = time.time()
        try:
            yield row
            row["status"] = "ok"
        except Exception as e:
            row["status"] = "error"
            row["error"] = f"{type(e).__name__}: {e}"
            row["traceback"] = traceback.format_exc(limit=5)
            raise
        finally:
            row["duration_s"] = round(time.time() - t0, 2)
            row["config"] = self.config_source
            self.rows.append(row)
            with open(self.path, "a", encoding="utf-8") as fh:
                fh.write(json.dumps(row, ensure_ascii=False, default=str) + "\n")

    def summary(self):
        """Filas de esta corrida, para mostrarlas en el notebook."""
        cols = ("stage", "status", "rows_read", "rows_written", "rows_rejected", "rows_duplicated", "duration_s")
        return [{c: r.get(c) for c in cols} for r in self.rows]
