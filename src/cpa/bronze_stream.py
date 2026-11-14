"""Ingesta streaming: eventos JSONL de Landing a Bronze con Structured Streaming (sección 7.1 del diseño).

  extract   readStream JSON con esquema explícito, PERMISSIVE y maxFilesPerTrigger (D4). Sin cleanSource:
            Landing no se toca; el checkpoint recuerda qué archivos ya se leyeron.
  transform ingest_ts (hora del micro-lote), source_file, dedupe_key, y deduplicación con watermark de
            1 hora sobre ingest_ts (D5)
  validate  dentro de cada micro-lote, separa las líneas ilegibles (_corrupt_record)
  load      escribe cada micro-lote en su propia carpeta ingest_date=D/batch_id=N, reemplazándola si existe:
            si Spark reintenta un micro-lote, lo reescribe en lugar de duplicarlo (CE3)

Con trigger(availableNow=True) procesa todo lo pendiente (120 archivos -> 12 micro-lotes) y termina.
"""
import time

from pyspark.sql import functions as F

from cpa import lake
from cpa.schemas import CORRUPT_COL, EVENTS_DIR, USAGE_EVENTS

# Columnas del registro original: con ellas se arma el hash de dedupe_key cuando no hay event_id.
SOURCE_COLS = [f.name for f in USAGE_EVENTS.fields]


def extract(spark, cfg):
    return (spark.readStream
            .format("json")
            .schema(USAGE_EVENTS)
            .option("mode", "PERMISSIVE")
            .option("columnNameOfCorruptRecord", CORRUPT_COL)
            .option("maxFilesPerTrigger", cfg.get("streaming", "max_files_per_trigger"))
            .load(lake.zone_path(cfg, "landing", EVENTS_DIR)))


def transform(df, cfg):
    return (df
            # _metadata solo existe sobre la fuente: source_file se calcula antes que cualquier otra cosa.
            .withColumn("source_file", lake.source_file_col(cfg))
            .withColumn("ingest_ts", F.current_timestamp())     # una misma hora para todo el micro-lote
            # Spark trata a todos los nulos como iguales: si la clave fuera solo event_id, todos los
            # registros sin id (o ilegibles) se juntarían en uno y se perderían. Sin id, la clave es un hash
            # del archivo y del contenido, así solo se descarta una copia idéntica del mismo archivo.
            .withColumn("dedupe_key", F.coalesce(
                F.col("event_id"),
                F.sha2(F.concat_ws("||", F.col("source_file"), F.to_json(F.struct(*SOURCE_COLS))), 256)))
            # Watermark sobre la hora de ingesta, no sobre la del evento: cada archivo trae eventos de los
            # 60 días y un watermark sobre `timestamp` descartaría el 91,6 % como tardío (D5). Acota el estado
            # de la deduplicación a una hora; los eventos atrasados por fecha entran al recalcular Silver.
            .withWatermark("ingest_ts", cfg.get("streaming", "dedupe_watermark"))
            .dropDuplicatesWithinWatermark(["dedupe_key"]))


def validate(batch_df):
    """(válidas, ilegibles) de un micro-lote. El micro-lote ya está persistido, así que se puede filtrar
    por _corrupt_record."""
    valid = batch_df.where(F.col(CORRUPT_COL).isNull()).drop(CORRUPT_COL)
    rejected = (batch_df.where(F.col(CORRUPT_COL).isNotNull())
                .select(F.col(CORRUPT_COL).alias("raw_record"), "source_file", "ingest_ts", "dedupe_key",
                        F.array(F.lit("parse_error")).alias("dq_errors")))
    return valid, rejected


def make_batch_writer(cfg, stats):
    """foreachBatch: corre en el driver una vez por micro-lote. `stats` junta los conteos de cada lote."""
    def write_batch(batch_df, batch_id):
        batch_df.persist()
        n = batch_df.count()
        written = rejected_n = 0
        if n > 0:
            ingest_date = batch_df.select(F.date_format("ingest_ts", "yyyy-MM-dd")).first()[0]
            folder = (f"ingest_date={ingest_date}", f"batch_id={batch_id}")
            valid, rejected = validate(batch_df)
            # ingest_date y batch_id quedan en la ruta (columnas de partición), no dentro del archivo.
            lake.write_parquet(cfg, valid, lake.zone_path(cfg, "bronze", "usage_events", *folder), num_files=1)
            written = valid.count()
            rejected_n = rejected.count()
            if rejected_n:
                lake.write_parquet(cfg, rejected, lake.zone_path(cfg, "quarantine", "usage_events", *folder),
                                   num_files=1)
        stats.append({"batch_id": batch_id, "rows": n, "written": written, "rejected": rejected_n})
        batch_df.unpersist()
    return write_batch


def progress_rows(query):
    """Métricas de cada micro-lote desde recentProgress (filas leídas, watermark, estado y descartes)."""
    rows = []
    for p in query.recentProgress:
        op = p["stateOperators"][0] if p.get("stateOperators") else {}
        rows.append({"batch_id": p["batchId"],
                     "input_rows": p["numInputRows"],
                     "watermark": p.get("eventTime", {}).get("watermark"),
                     "state_rows": op.get("numRowsTotal"),
                     "dropped_by_watermark": op.get("numRowsDroppedByWatermark", 0),
                     "dropped_duplicates": op.get("customMetrics", {}).get("numDroppedDuplicateRows", 0),
                     "duration_ms": p.get("durationMs", {}).get("triggerExecution")})
    return rows


def run_bronze_stream(spark, cfg, run_log, stage_name="bronze_stream"):
    """Ejecuta el stream con availableNow y espera a que termine. Devuelve la fila del run_log."""
    with run_log.stage(stage_name) as st:
        stats = []
        t0 = time.time()
        query = (transform(extract(spark, cfg), cfg).writeStream
                 .queryName(stage_name)
                 .foreachBatch(make_batch_writer(cfg, stats))
                 .option("checkpointLocation", lake.zone_path(cfg, "checkpoints", "bronze_usage_events"))
                 .trigger(availableNow=True)
                 .start())
        query.awaitTermination()
        progress = progress_rows(query)
        by_batch = {s["batch_id"]: s for s in stats}
        for p in progress:
            s = by_batch.get(p["batch_id"], {})
            p.update({"written": s.get("written", 0), "rejected": s.get("rejected", 0)})
        st.update({
            "rows_read": sum(p["input_rows"] for p in progress),
            "rows_written": sum(s["written"] for s in stats),
            "rows_rejected": sum(s["rejected"] for s in stats),
            "rows_duplicated": sum(p["dropped_duplicates"] for p in progress),
            "rows_dropped_by_watermark": sum(p["dropped_by_watermark"] for p in progress),
            "micro_batches_with_data": sum(1 for p in progress if p["input_rows"] > 0),
            "progress": progress,
            "stream_seconds": round(time.time() - t0, 2),
        })
    return st


def checkpoint_listing(cfg):
    """Contenido del checkpoint (archivos por subcarpeta), como evidencia de qué guarda el stream."""
    import os
    root = lake.zone_path(cfg, "checkpoints", "bronze_usage_events")
    out = {}
    for d, _, files in os.walk(root):
        visible = sorted(f for f in files if not f.startswith("."))
        if visible:
            out[os.path.relpath(d, root)] = visible
    return out
