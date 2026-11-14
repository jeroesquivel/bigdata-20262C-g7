"""Reglas de calidad de los eventos (src/cpa/quality.py)."""
from datetime import datetime

from pyspark.sql import functions as F

from cpa import quality


def by_id(df, *cols):
    return {r["event_id"]: tuple(r[c] for c in cols) for r in df.collect()}


def test_r1_event_id_nulo_es_error(make_events):
    df = quality.r1_event_id_not_null(make_events({"event_id": "e1"}, {"event_id": None}))
    assert sorted(r["err_missing_event_id"] for r in df.collect()) == [False, True]


def test_r3_timestamp_faltante_o_del_futuro(make_events):
    df = quality.r3_timestamp_valid(make_events(
        {"event_id": "ok"},
        {"event_id": "sin_ts", "timestamp": None},
        {"event_id": "dentro_tolerancia", "timestamp": datetime(2025, 9, 1, 0, 4)},   # 4 min después de la ingesta
        {"event_id": "futuro", "timestamp": datetime(2025, 9, 1, 0, 6)}), "5 minutes")
    got = by_id(df, "err_invalid_timestamp", "err_future_timestamp")
    assert got == {"ok": (False, False), "sin_ts": (True, False),
                   "dentro_tolerancia": (False, False), "futuro": (False, True)}


def test_r5_umbral_de_costo_negativo(make_events):
    """El umbral es -0,01 (config), no cero: -0,005 no se marca."""
    df = quality.r5_negative_cost(make_events(
        {"event_id": "pos", "cost_usd_increment": 2.0},
        {"event_id": "casi_cero", "cost_usd_increment": -0.005},
        {"event_id": "en_el_umbral", "cost_usd_increment": -0.01},
        {"event_id": "negativo", "cost_usd_increment": -5.0}), -0.01)
    got = {k: v[0] for k, v in by_id(df, "is_negative_cost").items()}
    assert got == {"pos": False, "casi_cero": False, "en_el_umbral": False, "negativo": True}


def test_r6_unit_se_completa_desde_metric(make_events):
    df = quality.r6_unit_present(make_events(
        {"event_id": "con_unit"},
        {"event_id": "cpu", "metric": "cpu_hours", "unit": None},
        {"event_id": "sin_value", "value": None, "unit": None},
        {"event_id": "desconocida", "metric": "gpu_hours", "unit": None}))
    got = by_id(df, "unit", "unit_imputed", "err_unknown_metric")
    assert got["con_unit"] == ("count", False, False)
    assert got["cpu"] == ("hours", True, False)
    assert got["sin_value"] == (None, False, False)       # sin value no hace falta unit
    assert got["desconocida"] == (None, False, True)      # métrica sin unidad conocida: quarantine


def test_r7_value_texto_se_convierte_o_se_marca(make_events):
    df = quality.r7_value_numeric(make_events(
        {"event_id": "num", "value": "95.0"}, {"event_id": "txt", "value": "abc"}, {"event_id": "nulo", "value": None}))
    got = by_id(df, "value_num", "value_cast_failed")
    assert got == {"num": (95.0, False), "txt": (None, True), "nulo": (None, False)}


def test_dq_errors_lista_solo_las_reglas_bloqueantes(make_events):
    df = quality.r6_unit_present(quality.r1_event_id_not_null(make_events(
        {"event_id": "ok"}, {"event_id": None, "metric": "gpu_hours", "unit": None})))
    df = quality.r3_timestamp_valid(df, "5 minutes").withColumn("_org_found", F.lit(True)).withColumn("_resource_found", F.lit(True))
    df = quality.with_dq_errors(quality.r4_keys_exist(df))
    errores = sorted(tuple(r["dq_errors"]) for r in df.collect())
    assert errores == [(), ("R1_missing_event_id", "R6_unknown_metric_unit")]
