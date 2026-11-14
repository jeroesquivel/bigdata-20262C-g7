"""Gold: mart FinOps org_daily_usage_by_service (sección 8 del diseño).

Grano: una fila por (org_id, usage_date, service). Es un groupBy con agregados que Spark ejecuta como
MapReduce: HashAggregate parcial (combiner) -> Exchange hashpartitioning (shuffle, 8 particiones)
-> HashAggregate final (reduce).
"""
from pyspark.sql import functions as F

from cpa import lake

GRAIN = ["org_id", "usage_date", "service"]


def metric_sum(metric):
    """Suma de value_num de una métrica; 0 si el grupo no tiene esa métrica (o si todos sus valores son nulos)."""
    return F.coalesce(F.sum(F.when(F.col("metric") == metric, F.col("value_num"))), F.lit(0.0))


def build_org_daily_usage(silver_events):
    """Los costos negativos (R5, menores a -0,01 según la config) van aparte y no se suman en
    daily_cost_usd (A2). Los que están entre -0,01 y 0 sí se suman: el umbral sale de la config, no de cero.
    genai_tokens y carbon_kg quedan nulos si ningún evento del grupo los midió (eventos v1, D14):
    "no se midió" no es lo mismo que cero. F.sum ya devuelve nulo cuando todos los valores son nulos."""
    neg = F.col("is_negative_cost")
    return (silver_events
            .withColumn("usage_date", F.col("event_date"))
            .groupBy(*GRAIN)
            .agg(F.round(F.sum(F.when(~neg, F.col("cost_usd_increment")).otherwise(0.0)), 6).alias("daily_cost_usd"),
                 F.round(F.sum(F.when(neg, F.col("cost_usd_increment")).otherwise(0.0)), 6).alias("negative_cost_usd"),
                 F.sum(neg.cast("int")).alias("negative_cost_events"),
                 metric_sum("requests").alias("requests"),
                 metric_sum("cpu_hours").alias("cpu_hours"),
                 metric_sum("storage_gb_hours").alias("storage_gb_hours"),
                 F.sum("genai_tokens").alias("genai_tokens"),
                 F.round(F.sum("carbon_kg"), 6).alias("carbon_kg"),
                 F.count("*").alias("event_count")))


def validate(spark, cfg, silver_events):
    """Unicidad del grano y conciliación del costo contra Silver."""
    mart = spark.read.parquet(lake.zone_path(cfg, "gold", "org_daily_usage_by_service"))
    rows = mart.count()
    distinct_keys = mart.select(*GRAIN).distinct().count()
    g = mart.agg(F.sum("daily_cost_usd"), F.sum("negative_cost_usd"), F.sum("negative_cost_events"),
                 F.sum("event_count")).first()
    s = silver_events.agg(F.sum("cost_usd_increment"), F.sum(F.col("is_negative_cost").cast("int")), F.count("*")).first()
    diff = abs((g[0] + g[1]) - s[0])
    return {"rows": rows, "distinct_keys": distinct_keys, "grain_unique": rows == distinct_keys,
            "gold_cost_total": round(g[0] + g[1], 4), "silver_cost_total": round(s[0], 4),
            "cost_diff": round(diff, 6), "cost_reconciles": diff < 0.01,   # tolerancia por el redondeo a 6 decimales
            "gold_negative_events": g[2], "silver_negative_events": s[1],
            "gold_event_count": g[3], "silver_events": s[2]}


def run_gold(spark, cfg, run_log):
    with run_log.stage("gold.org_daily_usage_by_service") as st:
        silver_events = spark.read.parquet(lake.zone_path(cfg, "silver", "usage_events"))
        mart = build_org_daily_usage(silver_events)
        # Un solo archivo: con unas 11.000 filas, los 8 archivos del shuffle serían archivos chicos.
        lake.write_parquet(cfg, mart, lake.zone_path(cfg, "gold", "org_daily_usage_by_service"),
                           num_files=1, replace_all=True)
        checks = validate(spark, cfg, silver_events)
        st.update({"rows_read": checks["silver_events"], "rows_written": checks["rows"], **checks})
    return {"checks": checks, "mart_df": mart, "stage": st}
