"""Serving en AstraDB (D9, D25).

- El esquema (cql/01_schema.cql) se ejecuta con cassandra-driver, un cliente CQL de Python.
- El mart de Gold se carga con spark-cassandra-connector: Spark escribe el DataFrame en paralelo, por
  partición. En Cassandra un INSERT es un upsert por clave primaria, así que re-cargar no duplica (D7).
- P1 y P2 se consultan con el driver, con sentencias preparadas: el valor de cada parámetro viaja aparte
  del texto CQL.
"""
import os
import re
from collections import defaultdict
from datetime import date, timedelta
from pathlib import Path

from pyspark.sql import functions as F

from cpa import lake
from cpa.config import REPO_ROOT, astra_secrets

TABLE = "org_daily_usage_by_service"


def astra_available():
    """True si están el token y el bundle (en Colab se cargan desde Colab Secrets)."""
    bundle = os.environ.get("ASTRA_DB_BUNDLE_PATH")
    return bool(os.environ.get("ASTRA_DB_TOKEN") and bundle and Path(bundle).exists())


def cql_session(cfg):
    from cassandra.auth import PlainTextAuthProvider
    from cassandra.cluster import Cluster
    token, bundle = astra_secrets()
    # AstraDB habla el protocolo v4; fijarlo evita que el driver pruebe versiones más nuevas y avise al bajar.
    cluster = Cluster(cloud={"secure_connect_bundle": bundle, "connect_timeout": 30}, protocol_version=4,
                      auth_provider=PlainTextAuthProvider("token", token))
    return cluster, cluster.connect(cfg.get("serving", "keyspace"))


def run_cql_file(session, path):
    """Ejecuta un archivo .cql sentencia por sentencia (separadas por ';'), ignorando los comentarios."""
    text = re.sub(r"--[^\n]*", "", Path(path).read_text(encoding="utf-8"))
    statements = [s.strip() for s in text.split(";") if s.strip()]
    for stmt in statements:
        session.execute(stmt)
    return len(statements)


def load_mart(spark, cfg):
    """Carga gold/org_daily_usage_by_service en la tabla de AstraDB y devuelve las filas leídas de vuelta."""
    keyspace = cfg.get("serving", "keyspace")
    mart = spark.read.parquet(lake.zone_path(cfg, "gold", TABLE))
    (mart.write.format("org.apache.spark.sql.cassandra")
         .options(keyspace=keyspace, table=TABLE)
         .mode("append")           # append = upsert por clave primaria en Cassandra
         .save())
    astra = spark.read.format("org.apache.spark.sql.cassandra").options(keyspace=keyspace, table=TABLE).load()
    return mart.count(), astra.count()


def query_p1(session, org_id, start, end):
    """P1: costos y requests diarios por servicio de una organización en [start, end]."""
    stmt = session.prepare(
        f"SELECT org_id, usage_date, service, daily_cost_usd, negative_cost_usd, requests, event_count "
        f"FROM {TABLE} WHERE org_id = ? AND usage_date >= ? AND usage_date <= ?")
    # usage_date llega como cassandra.util.Date; se pasa a texto ISO para mostrarlo y guardarlo.
    return [{**r._asdict(), "usage_date": str(r.usage_date)} for r in session.execute(stmt, (org_id, start, end))]


def query_p2(session, org_id, as_of, days=14, top_n=None):
    """P2: servicios con más costo en los `days` días hasta as_of (incluido).
    Cassandra devuelve las filas de la partición en el rango; la suma por servicio y el orden van en el cliente (A4)."""
    start = as_of - timedelta(days=days - 1)
    stmt = session.prepare(f"SELECT service, usage_date, daily_cost_usd FROM {TABLE} "
                           f"WHERE org_id = ? AND usage_date >= ? AND usage_date <= ?")
    rows = list(session.execute(stmt, (org_id, start, as_of)))
    totals, days_seen = defaultdict(float), defaultdict(int)
    for r in rows:
        totals[r.service] += r.daily_cost_usd or 0.0
        days_seen[r.service] += 1
    ranking = sorted(({"service": s, "cost_usd": round(c, 4), "dias_con_uso": days_seen[s]} for s, c in totals.items()),
                     key=lambda x: x["cost_usd"], reverse=True)
    return {"desde": str(start), "hasta": str(as_of), "filas_leidas": len(rows), "ranking": ranking[:top_n] if top_n else ranking}


def as_date(value):
    return value if isinstance(value, date) else date.fromisoformat(str(value))


def run_serving(spark, cfg, run_log, org_id, p1_range):
    """Esquema, carga y consultas P1 y P2. Devuelve conteos y resultados para la evidencia."""
    with run_log.stage("serving.astradb") as st:
        cluster, session = cql_session(cfg)
        try:
            st["cql_statements"] = run_cql_file(session, REPO_ROOT / "cql" / "01_schema.cql")
            gold_rows, astra_rows = load_mart(spark, cfg)
            as_of = as_date(cfg.get("analytics", "as_of_date"))
            p1 = query_p1(session, org_id, as_date(p1_range[0]), as_date(p1_range[1]))
            p2 = query_p2(session, org_id, as_of)
            # Control: lo que devuelve AstraDB para P1 coincide con Gold para la misma organización y rango.
            gold_p1 = (spark.read.parquet(lake.zone_path(cfg, "gold", TABLE))
                       .where((F.col("org_id") == org_id) & F.col("usage_date").between(*p1_range)).count())
            st.update({"rows_read": gold_rows, "rows_written": astra_rows, "astra_equals_gold": gold_rows == astra_rows,
                       "p1_rows": len(p1), "p1_rows_gold": gold_p1, "p2_rows_read": p2["filas_leidas"]})
        finally:
            cluster.shutdown()
    return {"stage": st, "p1": p1, "p2": p2}
