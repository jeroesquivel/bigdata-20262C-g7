"""Ingesta batch: CSV de Landing a Bronze (sección 7.2 del diseño).

Cada fuente pasa por las mismas cuatro funciones:
  extract   lee el CSV con su esquema explícito, en modo PERMISSIVE
  transform agrega las columnas técnicas ingest_ts, source_file e ingest_date
  validate  separa las filas ilegibles (_corrupt_record) de las válidas
  load      escribe las válidas en bronze/<fuente>/ingest_date=D/ y las ilegibles en quarantine/
Bronze guarda el dato tal como llegó (solo tipado): la limpieza y las reglas de negocio son de Silver.
"""
from datetime import datetime, timezone
from pathlib import Path

from pyspark.sql import functions as F

from cpa import lake
from cpa.schemas import CORRUPT_COL, CSV_SOURCES


def landing_rows(cfg, file_name):
    """Filas de datos del CSV contadas como líneas de texto, sin Spark (referencia para la conservación)."""
    with open(Path(cfg.paths["landing"]) / file_name, encoding="utf-8") as fh:
        return sum(1 for line in fh if line.strip()) - 1   # menos el encabezado


def extract(spark, cfg, file_name, schema):
    return (spark.read
            .schema(schema)
            .option("header", True)
            .option("mode", "PERMISSIVE")                    # una fila mal formada no corta la carga
            .option("columnNameOfCorruptRecord", CORRUPT_COL)
            .option("escape", '"')                           # tags_json trae comillas dobles escapadas ("")
            .csv(str(Path(cfg.paths["landing"]) / file_name)))


def transform(df, cfg, ingest_ts):
    # ingest_ts es el mismo para todas las fuentes de una corrida: así se sabe qué se cargó junto.
    return (df
            .withColumn("source_file", lake.source_file_col(cfg))
            .withColumn("ingest_ts", F.lit(ingest_ts).cast("timestamp"))
            .withColumn("ingest_date", F.to_date("ingest_ts")))


def validate(df):
    """Devuelve (válidas, ilegibles). Spark exige cachear antes de filtrar por _corrupt_record."""
    df = df.cache()
    valid = df.where(F.col(CORRUPT_COL).isNull()).drop(CORRUPT_COL)
    rejected = (df.where(F.col(CORRUPT_COL).isNotNull())
                .select(F.col(CORRUPT_COL).alias("raw_record"), "source_file", "ingest_ts", "ingest_date",
                        F.array(F.lit("parse_error")).alias("dq_errors")))
    return df, valid, rejected


def load(cfg, name, valid, rejected):
    # Un archivo por partición (los maestros pesan pocos KB) y overwrite dinámico: re-ejecutar el mismo día
    # reemplaza la partición ingest_date=D en lugar de duplicarla (D7).
    lake.write_parquet(cfg, valid, lake.zone_path(cfg, "bronze", name), partition_by=["ingest_date"], num_files=1)
    n_rejected = rejected.count()
    if n_rejected:
        lake.write_parquet(cfg, rejected, lake.zone_path(cfg, "quarantine", name),
                           partition_by=["ingest_date"], num_files=1)
    return n_rejected


def ingest_source(spark, cfg, run_log, name, ingest_ts):
    file_name, schema, _ = CSV_SOURCES[name]
    with run_log.stage(f"bronze_batch.{name}") as st:
        df, valid, rejected = validate(transform(extract(spark, cfg, file_name, schema), cfg, ingest_ts))
        n_rejected = load(cfg, name, valid, rejected)
        ingest_date = ingest_ts[:10]
        partition = lake.zone_path(cfg, "bronze", name, f"ingest_date={ingest_date}")
        st.update({
            "rows_read": landing_rows(cfg, file_name),
            "rows_written": spark.read.parquet(partition).count(),   # se cuenta lo escrito, no lo calculado
            "rows_rejected": n_rejected,
            "rows_duplicated": 0,          # Bronze no deduplica: la unicidad por clave natural es de Silver
            "ingest_date": ingest_date,
            **{f"output_{k}": v for k, v in lake.path_stats(partition).items() if k != "path"},
        })
        df.unpersist()
    return st   # fuera del with: ya tiene duration_s y status


def run_bronze_batch(spark, cfg, run_log, sources=None):
    """Ingesta todas las fuentes CSV (o las indicadas) con un mismo ingest_ts."""
    ingest_ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    return [ingest_source(spark, cfg, run_log, name, ingest_ts) for name in (sources or CSV_SOURCES)]
