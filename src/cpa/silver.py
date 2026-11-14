"""Silver: dimensiones y eventos conformados (secciones 7.2 y 7.3 del diseño).

dim_org y dim_resource salen del último snapshot de Bronze, deduplicadas por clave natural (SCD tipo 1, D12).
usage_events sale de todo Bronze (todas las corridas y micro-lotes):
  extract   lee Bronze de eventos y las dimensiones
  transform join broadcast con las dimensiones y reglas R1-R7 y R13 (quality.py)
  validate  separa lo que falla una regla bloqueante (va a quarantine con dq_errors) y deduplica por event_id (R2)
  load      escribe silver/usage_events particionado por event_date y quarantine/silver_usage_events
Conservación (CE1): filas de Bronze = Silver + quarantine + duplicados descartados.
"""
from pyspark.sql import Window
from pyspark.sql import functions as F

from cpa import lake, quality

# Columnas de las dimensiones que se agregan a cada evento (no van datos personales).
ORG_COLS = ["industry", "plan_tier", "hq_region"]


def latest_snapshot(spark, cfg, source, key):
    """Último ingest_date de un maestro en Bronze, con una fila por clave (gana la ingesta más reciente)."""
    df = spark.read.parquet(lake.zone_path(cfg, "bronze", source))
    last = df.agg(F.max("ingest_date")).first()[0]
    w = Window.partitionBy(*key).orderBy(F.col("ingest_ts").desc())
    return (df.where(F.col("ingest_date") == last)
              .withColumn("_rn", F.row_number().over(w)).where("_rn = 1").drop("_rn"))


def build_dim_org(spark, cfg):
    return (latest_snapshot(spark, cfg, "customers_orgs", ["org_id"])
            .select("org_id", "org_name", "industry", "hq_region", "plan_tier", "is_enterprise", "signup_date",
                    "sales_rep", "lifecycle_stage", "marketing_source", "nps_score",
                    F.col("ingest_date").alias("snapshot_date")))


def build_dim_resource(spark, cfg):
    return (latest_snapshot(spark, cfg, "resources", ["resource_id"])
            .select("resource_id", "org_id", "service", "region", "created_at", "state", "tags_json",
                    F.col("ingest_date").alias("snapshot_date")))


def enrich(events, dim_org, dim_resource):
    """Join con las dimensiones. Son chicas (80 y 400 filas), así que se mandan enteras a cada tarea con
    broadcast y no hay shuffle de los eventos (el equivalente a la caché distribuida de MapReduce).
    Es un left join: un evento sin organización o recurso no se pierde, lo marca R4."""
    org = dim_org.select("org_id", *ORG_COLS, F.lit(True).alias("_org_found"))
    res = dim_resource.select("resource_id", F.col("created_at").alias("resource_created_at"),
                              F.col("state").alias("resource_state"), F.lit(True).alias("_resource_found"))
    return (events.join(F.broadcast(org), "org_id", "left")
                  .join(F.broadcast(res), "resource_id", "left"))


def split_and_dedupe(checked):
    """(silver, quarantine, duplicados). Deduplica por event_id sobre todo Bronze (R2): se queda la primera
    ingesta de cada evento; Bronze ya deduplicó dentro de cada corrida del stream y esto cubre reprocesos."""
    rejected = checked.where(F.size("dq_errors") > 0)
    valid = checked.where(F.size("dq_errors") == 0)
    w = Window.partitionBy("event_id").orderBy("ingest_ts", "batch_id", "source_file")
    ranked = valid.withColumn("_rn", F.row_number().over(w))
    return ranked.where("_rn = 1").drop("_rn"), rejected, ranked.where("_rn > 1")


SILVER_COLS = ["event_id", "event_date", "timestamp", "org_id", "resource_id", "service", "region", "metric",
               "value", "value_num", "unit", "cost_usd_increment", "schema_version", "carbon_kg", "genai_tokens",
               *ORG_COLS, "resource_created_at", "resource_state",
               "is_negative_cost", "unit_imputed", "value_cast_failed", "is_before_resource_created",
               "source_file", "ingest_ts", "ingest_date", "batch_id"]


def run_silver(spark, cfg, run_log):
    """Construye dim_org, dim_resource y usage_events. Devuelve un dict con conteos, reporte y DataFrames."""
    out = {}
    with run_log.stage("silver.dims") as st:
        dim_org, dim_resource = build_dim_org(spark, cfg), build_dim_resource(spark, cfg)
        for name, df in (("dim_org", dim_org), ("dim_resource", dim_resource)):
            lake.write_parquet(cfg, df, lake.zone_path(cfg, "silver", name), num_files=1)
        dim_org = spark.read.parquet(lake.zone_path(cfg, "silver", "dim_org"))
        dim_resource = spark.read.parquet(lake.zone_path(cfg, "silver", "dim_resource"))
        st.update({"dim_org": dim_org.count(), "dim_resource": dim_resource.count()})
        st["rows_written"] = st["dim_org"] + st["dim_resource"]

    with run_log.stage("silver.usage_events") as st:
        bronze = spark.read.parquet(lake.zone_path(cfg, "bronze", "usage_events"))
        checked = quality.apply_rules(enrich(bronze, dim_org, dim_resource), cfg)
        checked = checked.withColumn("event_date", F.to_date("timestamp")).cache()
        silver, rejected, duplicates = split_and_dedupe(checked)

        # Un archivo por día (repartition por la columna de partición) y la tabla completa reemplazada.
        lake.write_parquet(cfg, silver.select(*SILVER_COLS).repartition("event_date"),
                           lake.zone_path(cfg, "silver", "usage_events"), partition_by=["event_date"], replace_all=True)
        # Quarantine guarda el registro tal como estaba en Bronze más dq_errors (sección 6.1).
        lake.write_parquet(cfg, rejected.select(*bronze.columns, "dq_errors"),
                           lake.zone_path(cfg, "quarantine", "silver_usage_events"), num_files=1, replace_all=True)

        n_bronze = bronze.count()
        n_silver = spark.read.parquet(lake.zone_path(cfg, "silver", "usage_events")).count()
        n_rejected = rejected.count()
        n_dup = duplicates.count()
        report = quality.quality_report(checked, n_dup)
        st.update({"rows_read": n_bronze, "rows_written": n_silver, "rows_rejected": n_rejected,
                   "rows_duplicated": n_dup, "conserva": n_bronze == n_silver + n_rejected + n_dup,
                   "quality_report": report})
        out.update({"checked": checked, "report": report, "conteos": dict(st), "dim_org": dim_org,
                    "dim_resource": dim_resource, "enriched_plan": enrich(bronze, dim_org, dim_resource)})
    out["conteos"].update(duration_s=st["duration_s"])
    return out
