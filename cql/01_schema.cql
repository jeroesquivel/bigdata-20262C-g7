-- Esquema de serving en AstraDB (D25). El keyspace `cpa` se crea desde el portal de AstraDB:
-- AstraDB no acepta CREATE KEYSPACE por CQL.
-- Se ejecuta con src/cpa/serving.py (cassandra-driver) o pegándolo en la consola CQL del portal.

-- Mart FinOps: una fila por organización, día y servicio (grano de gold/org_daily_usage_by_service).
-- Partition key org_id: P1 y P2 filtran por organización y leen una sola partición.
-- Clustering usage_date DESC: el rango de fechas se resuelve dentro de la partición, de lo más nuevo a lo más viejo.
CREATE TABLE IF NOT EXISTS cpa.org_daily_usage_by_service (
    org_id               text,
    usage_date           date,
    service              text,
    daily_cost_usd       double,   -- costo del día sin los costos menores a -0,01 (R5)
    negative_cost_usd    double,   -- suma de los costos menores a -0,01
    negative_cost_events bigint,
    requests             double,
    cpu_hours            double,
    storage_gb_hours     double,
    genai_tokens         bigint,   -- nulo si ningún evento del día lo midió (v1)
    carbon_kg            double,   -- nulo si ningún evento del día lo midió (v1)
    event_count          bigint,
    PRIMARY KEY ((org_id), usage_date, service)
) WITH CLUSTERING ORDER BY (usage_date DESC, service ASC);
