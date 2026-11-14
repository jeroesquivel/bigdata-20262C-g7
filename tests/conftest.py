"""Fixtures de pytest: una sola SparkSession local para todas las pruebas y la configuración del proyecto."""
import sys
from datetime import datetime
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT / "src"))

from cpa.config import load_config  # noqa: E402
from cpa.spark_session import get_spark  # noqa: E402


@pytest.fixture(scope="session")
def cfg():
    return load_config(REPO_ROOT / "config" / "config.example.yaml")


@pytest.fixture(scope="session")
def spark(cfg):
    session = get_spark(cfg)
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


# Evento base válido; cada prueba cambia solo los campos que le interesan.
BASE_EVENT = {"event_id": "e1", "timestamp": datetime(2025, 8, 1, 10, 0), "org_id": "org_a", "resource_id": "res_a",
              "service": "compute", "region": "us-east", "metric": "requests", "value": "10.0", "unit": "count",
              "cost_usd_increment": 1.0, "schema_version": 2, "carbon_kg": 0.01, "genai_tokens": None,
              "ingest_ts": datetime(2025, 9, 1, 0, 0)}

EVENT_DDL = ("event_id STRING, timestamp TIMESTAMP, org_id STRING, resource_id STRING, service STRING, region STRING, "
             "metric STRING, value STRING, unit STRING, cost_usd_increment DOUBLE, schema_version INT, "
             "carbon_kg DOUBLE, genai_tokens BIGINT, ingest_ts TIMESTAMP")


@pytest.fixture
def make_events(spark):
    """make_events(dict, dict, ...) -> DataFrame de eventos con el esquema de Bronze."""
    def _make(*overrides):
        rows = [{**BASE_EVENT, **o} for o in overrides]
        cols = [c.split()[0] for c in EVENT_DDL.split(", ")]
        return spark.createDataFrame([tuple(r[c] for c in cols) for r in rows], EVENT_DDL)
    return _make
