"""Gold: grano único, costos negativos aparte y nulos de v1 (src/cpa/gold.py)."""
from datetime import date

from cpa.gold import GRAIN, build_org_daily_usage

SILVER_DDL = ("org_id STRING, event_date DATE, service STRING, metric STRING, value_num DOUBLE, "
              "cost_usd_increment DOUBLE, is_negative_cost BOOLEAN, genai_tokens BIGINT, carbon_kg DOUBLE")


def test_mart_grano_y_agregados(spark):
    d = date(2025, 8, 1)
    rows = [
        ("org_a", d, "compute", "requests", 10.0, 1.0, False, None, 0.01),
        ("org_a", d, "compute", "cpu_hours", 2.0, 3.0, False, None, 0.02),
        ("org_a", d, "compute", "requests", 5.0, -4.0, True, None, None),    # costo negativo (R5): va aparte
        ("org_a", d, "compute", "requests", None, -0.005, False, None, None),  # entre -0,01 y 0: se suma
        ("org_a", d, "genai", "requests", 1.0, 2.0, False, 500, 0.1),
        ("org_b", d, "compute", "requests", 7.0, 1.0, False, None, None),     # evento v1: sin carbon_kg
    ]
    mart = build_org_daily_usage(spark.createDataFrame(rows, SILVER_DDL))
    assert mart.count() == mart.select(*GRAIN).distinct().count() == 3

    a = mart.where("org_id = 'org_a' AND service = 'compute'").first()
    assert a["daily_cost_usd"] == 3.995 and a["negative_cost_usd"] == -4.0 and a["negative_cost_events"] == 1
    assert (a["requests"], a["cpu_hours"], a["storage_gb_hours"], a["event_count"]) == (15.0, 2.0, 0.0, 4)
    assert a["carbon_kg"] == 0.03 and a["genai_tokens"] is None      # ningún evento de compute midió tokens

    assert mart.where("service = 'genai'").first()["genai_tokens"] == 500
    b = mart.where("org_id = 'org_b'").first()
    assert b["carbon_kg"] is None                                     # D14: "no se midió" no es cero
