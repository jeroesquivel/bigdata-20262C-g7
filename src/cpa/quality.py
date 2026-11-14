"""Reglas de calidad de los eventos (sección 6.5 del diseño, R1 a R7 y R13).

Cada regla es una función que recibe un DataFrame y le agrega una columna; ninguna borra filas
("marcar en vez de descartar"). Hay dos tipos de columnas:
  - Bloqueantes: `err_<regla>` (booleano). Si alguna es verdadera, el código de la regla va a `dq_errors`
    y el registro termina en quarantine, porque no se puede interpretar o vincular.
  - Flags: `is_negative_cost`, `unit_imputed`, `value_cast_failed`, `is_before_resource_created`.
    El registro sigue a Silver marcado, y Gold decide cómo usarlo.
R2 (event_id único) no es una columna: es la deduplicación de silver.py.
"""
from pyspark.sql import functions as F

# Cada métrica tiene siempre la misma unidad en los datos (D15); se usa para completar `unit` (R6).
METRIC_UNITS = {"requests": "count", "cpu_hours": "hours", "storage_gb_hours": "gb_hours"}

# Código que se escribe en dq_errors para cada columna bloqueante.
BLOCKING = {
    "err_missing_event_id": "R1_missing_event_id",
    "err_invalid_timestamp": "R3_invalid_timestamp",
    "err_future_timestamp": "R3_future_timestamp",
    "err_orphan_org": "R4_orphan_org_id",
    "err_orphan_resource": "R4_orphan_resource_id",
    "err_unknown_metric": "R6_unknown_metric_unit",
}
FLAGS = ["is_negative_cost", "unit_imputed", "value_cast_failed", "is_before_resource_created"]


def r1_event_id_not_null(df):
    return df.withColumn("err_missing_event_id", F.col("event_id").isNull())


def r3_timestamp_valid(df, tolerance):
    """El evento tiene timestamp y no es del futuro respecto de su ingesta (con tolerancia, D21)."""
    return (df
            .withColumn("err_invalid_timestamp", F.col("timestamp").isNull())
            .withColumn("err_future_timestamp",
                        F.coalesce(F.col("timestamp") > F.col("ingest_ts") + F.expr(f"INTERVAL {tolerance}"),
                                   F.lit(False))))


def r4_keys_exist(df):
    """Requiere que antes se haya hecho el join con las dimensiones (columnas _org_found y _resource_found)."""
    return (df
            .withColumn("err_orphan_org", F.col("_org_found").isNull())
            .withColumn("err_orphan_resource", F.col("_resource_found").isNull()))


def r5_negative_cost(df, tolerance):
    """Flag, no quarantine: el costo negativo puede ser un ajuste real. Umbral configurable (-0,01)."""
    return df.withColumn("is_negative_cost", F.coalesce(F.col("cost_usd_increment") < F.lit(tolerance), F.lit(False)))


def r6_unit_present(df):
    """Si hay `value` y falta `unit`, se completa desde `metric` y se marca (D15).
    Si la métrica no está en el mapa, no hay forma de saber la unidad: quarantine."""
    mapping = F.create_map(*[F.lit(x) for kv in METRIC_UNITS.items() for x in kv])
    needs_unit = F.col("value").isNotNull() & F.col("unit").isNull()
    imputed = mapping[F.col("metric")]
    return (df
            .withColumn("unit_imputed", needs_unit & imputed.isNotNull())
            .withColumn("err_unknown_metric", needs_unit & imputed.isNull())
            .withColumn("unit", F.when(needs_unit, imputed).otherwise(F.col("unit"))))


def r7_value_numeric(df):
    """`value` llega como texto (D13). try_cast devuelve nulo si no es un número en lugar de cortar el job;
    el registro se conserva con value_num nulo y el flag value_cast_failed."""
    return (df
            .withColumn("value_num", F.expr("try_cast(value AS DOUBLE)"))
            .withColumn("value_cast_failed", F.col("value").isNotNull() & F.col("value_num").isNull()))


def r13_before_resource_created(df):
    """Flag: evento anterior al alta del recurso (D18). El maestro es una foto actual, así que puede ser
    un recurso que se volvió a crear; se marca y no se descarta."""
    return df.withColumn("is_before_resource_created",
                         F.coalesce(F.to_date("timestamp") < F.col("resource_created_at"), F.lit(False)))


def with_dq_errors(df):
    """Arma dq_errors con los códigos de las reglas bloqueantes que fallaron (vacío si ninguna)."""
    codes = [F.when(F.col(c), F.lit(code)) for c, code in BLOCKING.items()]
    return df.withColumn("dq_errors", F.array_compact(F.array(*codes)))


def apply_rules(df, cfg):
    tol_cost = float(cfg.get("analytics", "cost_negative_tolerance"))
    tol_future = cfg.get("analytics", "future_event_tolerance")
    df = r1_event_id_not_null(df)
    df = r3_timestamp_valid(df, tol_future)
    df = r4_keys_exist(df)
    df = r5_negative_cost(df, tol_cost)
    df = r6_unit_present(df)
    df = r7_value_numeric(df)
    df = r13_before_resource_created(df)
    return with_dq_errors(df)


def quality_report(checked_df, duplicates):
    """Conteo por regla: casos, tipo y acción. `checked_df` es el DataFrame con todas las columnas de reglas."""
    agg = checked_df.agg(*[F.sum(F.col(c).cast("int")).alias(c) for c in list(BLOCKING) + FLAGS]).first().asDict()
    rows = [{"regla": code, "tipo": "bloqueante", "accion": "quarantine", "casos": agg[c] or 0}
            for c, code in BLOCKING.items()]
    rows.append({"regla": "R2_duplicate_event_id", "tipo": "deduplicación", "accion": "se conserva una copia",
                 "casos": duplicates})
    actions = {"is_negative_cost": ("R5", "flag; va aparte en Gold"),
               "unit_imputed": ("R6", "se completa desde metric"),
               "value_cast_failed": ("R7", "value_num nulo"),
               "is_before_resource_created": ("R13", "flag")}
    rows += [{"regla": f"{actions[c][0]}_{c}", "tipo": "flag", "accion": actions[c][1], "casos": agg[c] or 0}
             for c in FLAGS]
    return rows
