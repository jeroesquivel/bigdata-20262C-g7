"""Score MAD sobre series conocidas (src/cpa/anomalies.py, D11)."""
from datetime import date, timedelta

import pytest

from cpa.anomalies import score

DDL = "org_id STRING, usage_date DATE, service STRING, daily_cost_usd DOUBLE"


def serie(org, service, values):
    return [(org, date(2025, 8, 1) + timedelta(days=i), service, float(v)) for i, v in enumerate(values)]


def test_score_mad_serie_conocida(spark):
    # Mediana 10; desvíos |x-10| = 0,1,1,0,2,2,0,90 -> MAD = mediana = 1.
    values = [10, 11, 9, 10, 12, 8, 10, 100]
    out = {r["daily_cost_usd"]: r for r in score(spark.createDataFrame(serie("o", "compute", values), DDL), 3.5).collect()}
    assert out[100.0]["median_cost"] == 10.0 and out[100.0]["mad_cost"] == 1.0
    assert out[100.0]["anomaly_score"] == pytest.approx(0.6745 * 90, abs=1e-3)   # 60,705
    assert out[100.0]["is_cost_anomaly"] is True
    assert out[12.0]["anomaly_score"] == pytest.approx(1.349, abs=1e-3) and out[12.0]["is_cost_anomaly"] is False


def test_mad_cero_usa_el_servicio_y_si_no_no_marca(spark):
    rows = (serie("plano", "storage", [5, 5, 5, 5, 50])          # MAD del par = 0 -> usa el del servicio
            + serie("otro", "storage", [1, 2, 3, 4, 5])
            + serie("constante", "dns", [3, 3, 3]))               # MAD del par y del servicio = 0
    out = score(spark.createDataFrame(rows, DDL), 3.5)
    plano = out.where("org_id = 'plano'").select("mad_scope").distinct().collect()
    assert [r[0] for r in plano] == ["service"]
    constante = out.where("org_id = 'constante'").collect()
    assert all(r["anomaly_score"] is None and r["is_cost_anomaly"] is False for r in constante)
