"""Orquestación del pipeline (D10): las etapas en orden, con una fila de run_log por etapa.

Si una etapa falla, queda registrada con status "error" y el error se propaga: las etapas siguientes
dependen de la anterior, así que no tiene sentido seguir.
"""
from pyspark.sql import functions as F

from cpa import lake
from cpa.anomalies import run_anomalies
from cpa.bronze_batch import run_bronze_batch
from cpa.bronze_stream import run_bronze_stream
from cpa.gold import run_gold
from cpa.schemas import CSV_SOURCES
from cpa.silver import run_silver

STAGES = ("bronze_batch", "bronze_stream", "silver", "gold", "anomalies", "serving")


def run_pipeline(spark, cfg, run_log, stages=STAGES, serving_args=None):
    """Corre las etapas pedidas. serving_args = (org_id, (desde, hasta)) para P1; sin él no se carga AstraDB."""
    results = {}
    if "bronze_batch" in stages:
        results["bronze_batch"] = run_bronze_batch(spark, cfg, run_log)
    if "bronze_stream" in stages:
        results["bronze_stream"] = run_bronze_stream(spark, cfg, run_log)
    if "silver" in stages:
        results["silver"] = run_silver(spark, cfg, run_log)
    if "gold" in stages:
        results["gold"] = run_gold(spark, cfg, run_log)
    if "anomalies" in stages:
        results["anomalies"] = run_anomalies(spark, cfg, run_log)
    if "serving" in stages and serving_args:
        from cpa.serving import run_serving
        results["serving"] = run_serving(spark, cfg, run_log, *serving_args)
    return results


def _table(spark, cfg, zone, *parts):
    path = lake.zone_path(cfg, zone, *parts)
    stats = lake.path_stats(path)
    rows = spark.read.parquet(path).count() if stats["files"] else 0
    return {"tabla": f"{zone}/{'/'.join(parts)}", "filas": rows, "archivos": stats["files"]}


def zone_snapshot(spark, cfg, with_astra=False):
    """Filas y archivos de cada tabla del lago (y de la tabla de AstraDB), para comparar dos corridas."""
    out = [_table(spark, cfg, "bronze", name) for name in CSV_SOURCES]
    out.append(_table(spark, cfg, "bronze", "usage_events"))
    out += [_table(spark, cfg, "silver", t) for t in ("dim_org", "dim_resource", "usage_events")]
    out.append(_table(spark, cfg, "quarantine", "silver_usage_events"))
    out += [_table(spark, cfg, "gold", t) for t in ("org_daily_usage_by_service", "cost_anomaly_mart")]
    # Una suma de control del mart: si cambiara algún valor (no solo la cantidad de filas), se vería acá.
    gold = spark.read.parquet(lake.zone_path(cfg, "gold", "org_daily_usage_by_service"))
    out.append({"tabla": "gold/org_daily_usage_by_service (suma daily_cost_usd)",
                "filas": round(gold.agg(F.sum("daily_cost_usd")).first()[0], 4), "archivos": None})
    if with_astra:
        astra = (spark.read.format("org.apache.spark.sql.cassandra")
                 .options(keyspace=cfg.get("serving", "keyspace"), table="org_daily_usage_by_service").load())
        out.append({"tabla": "astradb/cpa.org_daily_usage_by_service", "filas": astra.count(), "archivos": None})
    return out


def compare_snapshots(before, after):
    rows = []
    for b, a in zip(before, after):
        rows.append({"tabla": b["tabla"], "filas_antes": b["filas"], "filas_despues": a["filas"],
                     "archivos_antes": b["archivos"], "archivos_despues": a["archivos"],
                     "sin_cambios": b["filas"] == a["filas"] and b["archivos"] == a["archivos"]})
    return rows
