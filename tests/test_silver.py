"""Silver: deduplicación (R2), eventos v1 y el lote de prueba de punta a punta (D23)."""
from datetime import datetime

from pyspark.sql import functions as F

from cpa import lake
from cpa.bronze_batch import run_bronze_batch
from cpa.bronze_stream import run_bronze_stream
from cpa.config import REPO_ROOT, load_config
from cpa.run_log import RunLog
from cpa.silver import run_silver, split_and_dedupe


def test_r2_se_queda_la_primera_ingesta(make_events):
    df = (make_events({"event_id": "e1", "ingest_ts": datetime(2025, 9, 2), "cost_usd_increment": 2.0},
                      {"event_id": "e1", "ingest_ts": datetime(2025, 9, 1), "cost_usd_increment": 1.0},
                      {"event_id": "e2"})
          .withColumn("batch_id", F.lit(0)).withColumn("source_file", F.lit("f.jsonl"))
          .withColumn("dq_errors", F.array().cast("array<string>")))
    silver, rejected, duplicates = split_and_dedupe(df)
    assert silver.count() == 2 and rejected.count() == 0 and duplicates.count() == 1
    assert silver.where("event_id = 'e1'").first()["cost_usd_increment"] == 1.0   # la ingesta más vieja


def test_lote_de_prueba_de_punta_a_punta(spark, tmp_path):
    """El lote de tests/fixtures/ (ver su README) en un lago temporal: conteos y una muestra por regla."""
    cfg = load_config(REPO_ROOT / "tests" / "fixtures" / "config_fixture.yaml")
    for zone in ("bronze", "silver", "gold", "quarantine", "checkpoints", "meta"):
        cfg.paths[zone] = str(tmp_path / zone)       # Landing sigue siendo tests/fixtures/landing (solo lectura)
    log = RunLog(cfg)

    batch = {r["stage"]: r for r in run_bronze_batch(spark, cfg, log, sources=["customers_orgs", "resources"])}
    assert batch["bronze_batch.customers_orgs"]["rows_rejected"] == 1        # signup_date ilegible

    stream = run_bronze_stream(spark, cfg, log)
    assert (stream["rows_read"], stream["rows_written"], stream["rows_rejected"], stream["rows_duplicated"]) == (14, 12, 1, 1)

    res = run_silver(spark, cfg, log)
    c = res["conteos"]
    assert (c["rows_read"], c["rows_written"], c["rows_rejected"], c["conserva"]) == (12, 6, 6, True)
    casos = {r["regla"]: r["casos"] for r in res["report"]}
    for regla in ("R1_missing_event_id", "R3_invalid_timestamp", "R3_future_timestamp", "R4_orphan_org_id",
                  "R4_orphan_resource_id", "R6_unknown_metric_unit", "R5_is_negative_cost", "R6_unit_imputed",
                  "R7_value_cast_failed", "R13_is_before_resource_created"):
        assert casos[regla] == 1, regla

    silver = spark.read.parquet(lake.zone_path(cfg, "silver", "usage_events"))
    v1 = silver.where("event_id = 'fx_ok_v1'").first()
    assert v1["schema_version"] == 1 and v1["carbon_kg"] is None and v1["genai_tokens"] is None   # D14: nulo, no 0
