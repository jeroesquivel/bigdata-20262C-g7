"""Detección de anomalías de costo con z-score robusto (D11, D24).

Para cada par (organización, servicio) se toma la serie de daily_cost_usd del mart y se calcula
    anomaly_score = 0,6745 * (x - mediana) / MAD,   MAD = mediana(|x - mediana|)
y se marca is_cost_anomaly cuando |anomaly_score| supera el umbral (3,5 por defecto, configurable).
La mediana y el MAD no se mueven con los picos, a diferencia de la media y el desvío. El 0,6745 hace que,
con datos normales, el score sea comparable a un z-score clásico.
Si un par tiene MAD = 0 (todos sus días casi iguales) se usan la mediana y el MAD de todo el servicio;
si también es 0, el score queda nulo y no se marca.
"""
from pyspark.sql import functions as F

from cpa import lake

K = 0.6745


def _median_mad(df, keys, value="daily_cost_usd"):
    """Mediana exacta y MAD de `value` por `keys`."""
    med = df.groupBy(*keys).agg(F.expr(f"percentile({value}, 0.5)").alias("median_cost"))
    return (df.join(med, keys)
              .groupBy(*keys)
              .agg(F.first("median_cost").alias("median_cost"),
                   F.expr(f"percentile(abs({value} - median_cost), 0.5)").alias("mad_cost"),
                   F.count("*").alias("days")))


def score(mart, threshold):
    pair = _median_mad(mart, ["org_id", "service"])
    svc = (_median_mad(mart, ["service"])
           .select("service", F.col("median_cost").alias("svc_median"), F.col("mad_cost").alias("svc_mad")))
    use_pair = F.col("mad_cost") > 0
    return (mart.select("org_id", "usage_date", "service", "daily_cost_usd")
            .join(pair, ["org_id", "service"])
            .join(svc, "service")
            .withColumn("mad_scope", F.when(use_pair, "org_service").when(F.col("svc_mad") > 0, "service"))
            .withColumn("ref_median", F.when(use_pair, F.col("median_cost")).otherwise(F.col("svc_median")))
            .withColumn("ref_mad", F.when(use_pair, F.col("mad_cost")).otherwise(F.col("svc_mad")))
            .withColumn("anomaly_score",
                        F.when(F.col("ref_mad") > 0,
                               F.round(F.lit(K) * (F.col("daily_cost_usd") - F.col("ref_median")) / F.col("ref_mad"), 4)))
            .withColumn("is_cost_anomaly", F.coalesce(F.abs("anomaly_score") > F.lit(threshold), F.lit(False)))
            .withColumn("threshold", F.lit(float(threshold)))
            .select("org_id", "usage_date", "service", "daily_cost_usd", "days",
                    F.round("ref_median", 6).alias("median_cost"), F.round("ref_mad", 6).alias("mad_cost"),
                    "mad_scope", "anomaly_score", "is_cost_anomaly", "threshold"))


def sensitivity(scored, thresholds=(2.5, 3.5, 5.0)):
    total = scored.count()
    out = []
    for t in thresholds:
        n = scored.where(F.abs("anomaly_score") > t).count()
        n_pairs = scored.where(F.abs("anomaly_score") > t).select("org_id", "service").distinct().count()
        out.append({"umbral": t, "anomalias": n, "pct_filas": round(100 * n / total, 2), "pares_org_servicio": n_pairs})
    return out


def run_anomalies(spark, cfg, run_log):
    threshold = float(cfg.get("analytics", "anomaly_mad_threshold"))
    with run_log.stage("gold.cost_anomaly_mart") as st:
        mart = spark.read.parquet(lake.zone_path(cfg, "gold", "org_daily_usage_by_service"))
        lake.write_parquet(cfg, score(mart, threshold), lake.zone_path(cfg, "gold", "cost_anomaly_mart"),
                           num_files=1, replace_all=True)
        scored = spark.read.parquet(lake.zone_path(cfg, "gold", "cost_anomaly_mart"))
        n = scored.where("is_cost_anomaly").count()
        rows = scored.count()
        st.update({"rows_read": mart.count(), "rows_written": rows, "threshold": threshold, "anomalies": n,
                   "anomalies_pct": round(100 * n / rows, 2),
                   "pairs": scored.select("org_id", "service").distinct().count(),
                   "pairs_mad_zero": scored.where("mad_scope != 'org_service' OR mad_scope IS NULL")
                                           .select("org_id", "service").distinct().count(),
                   "sensitivity": sensitivity(scored)})
    return {"scored": scored, "stage": st}
