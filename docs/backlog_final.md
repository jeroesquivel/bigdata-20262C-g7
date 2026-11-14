# Backlog hacia la entrega final

Primer corte (14/11/2026). Se cierra en la clase del 16/11, cuando el profesor congela el alcance de la final; los cambios a partir de ese feedback se registran en [`plan_correcciones.md`](plan_correcciones.md).

Prioridades: **obligatorio** (lo pide la consigna para la final), **deseable** (mejora la solución si sobra tiempo) y **fuera de alcance** (descartado con justificación en `DECISIONS.md`).

| # | Prioridad | Ítem | Viene de | Responsable |
|---|---|---|---|---|
| B1 | Obligatorio | Silver de `billing_monthly` con las reglas R8 a R12 (moneda, tasa de cambio, notas de crédito, créditos mayores al subtotal) | Consigna 7.2 y 7.3; diseño 6.5; A3, A6 | Pedro Salinas |
| B2 | Obligatorio | Bronze ya ingesta las 7 fuentes; falta Silver de `support_tickets`, `nps_surveys` y `marketing_touches`, con R10, R13 y R14 | Consigna 7.2 y 7.3 | Pedro Salinas |
| B3 | Obligatorio | Reglas de `dim_org`: R10 (`nps_score` fuera de rango) y R15 (`is_enterprise` desde `plan_tier`) | Diseño 6.5; A7 | Pedro Salinas |
| B4 | Obligatorio | Marts `revenue_by_org_month`, `tickets_by_org_date`, `tickets_by_severity_date` y `genai_tokens_by_org_date` | Consigna 7.3; diseño 6.1 | Pedro Salinas |
| B5 | Obligatorio | Tablas de AstraDB y consultas P3, P4 y P5, con su justificación query-first y la matriz consulta-tabla completa | Consigna 7.4; diseño 4.3; A5 | Jerónimo Esquivel |
| B6 | Obligatorio | Publicar `cost_anomaly_mart` en AstraDB, ajustar el umbral de anomalías y documentar las limitaciones del método | D11, D24; consigna 7.2 | Pedro Salinas |
| B7 | Obligatorio | Linaje, observabilidad y seguridad completos: `users` en Silver sin email, retención aplicada, gobierno por cada tabla nueva | Consigna 7.2 y 7.6; `gobierno.md` | Jerónimo Esquivel |
| B8 | Obligatorio | Presentación ejecutiva y video explicativo (en clase se dijo que no hay defensa oral; confirmar) | Consigna 7.5 | Los tres |
| B9 | Obligatorio | Tags `v1.0-entrega1` y `v3.0-final` y cierre de las correcciones pendientes | `plan_correcciones.md` (C5) | Jerónimo Esquivel |
| B10 | Deseable | Tabla precalculada para el top-N de P2, en lugar de ordenar en el cliente | A4 | Jerónimo Esquivel |
| B11 | Deseable | Recalcular en Silver y Gold solo las fechas afectadas por cada carga, según el run log | D7; diseño 7.3 | Agustín Ronda |
| B12 | Deseable | Script de retención por zona y `StreamingQueryListener` para las métricas del stream | Diseño 6.3; clase de streaming | Agustín Ronda |
| B13 | Deseable | Parsear `tags_json` en `dim_resource` y marcar los recursos con `pii:true` | Diccionario de datos | Pedro Salinas |
| B14 | Fuera de alcance | Kafka, Delta Lake o Iceberg, un orquestador (Airflow), un clúster administrado, SCD tipo 2, modelos como Isolation Forest o Prophet | D1 a D4, D10 a D12 | |
