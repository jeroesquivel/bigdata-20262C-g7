-- Consultas mínimas de la entrega 2 sobre AstraDB (consigna 7.4, D25).
-- Los valores de ejemplo son la organización con más costo de la muestra y as_of_date = 2025-08-31
-- (analytics.as_of_date en config/config.example.yaml). Las dos leen una sola partición (org_id)
-- y filtran por rango sobre la clustering key, sin ALLOW FILTERING. Consistencia: LOCAL_QUORUM.
-- src/cpa/serving.py ejecuta estas mismas consultas con parámetros; acá están con valores fijos para la consola CQL.

-- P1. Costos y requests diarios de una organización por servicio en un rango de fechas.
SELECT org_id, usage_date, service, daily_cost_usd, negative_cost_usd, requests, event_count
FROM cpa.org_daily_usage_by_service
WHERE org_id = 'org_53lc58dr'
  AND usage_date >= '2025-08-01' AND usage_date <= '2025-08-14';

-- P2. Servicios con más costo de una organización en los últimos 14 días hasta as_of_date
-- (2025-08-18 a 2025-08-31, ambos incluidos). Cassandra devuelve como mucho 84 filas (14 días por 6 servicios);
-- la suma por servicio y el orden (top-N) se hacen en el cliente (A4), porque CQL no agrupa por una columna
-- que no es prefijo de la clave.
SELECT service, usage_date, daily_cost_usd
FROM cpa.org_daily_usage_by_service
WHERE org_id = 'org_53lc58dr'
  AND usage_date >= '2025-08-18' AND usage_date <= '2025-08-31';
