# Serving en AstraDB, corrida 20261114T002224Z

## Carga

| rows_read | rows_written | astra_equals_gold | p1_rows | p1_rows_gold | p2_rows_read | duration_s |
|---|---|---|---|---|---|---|
| 11050 | 11050 | True | 64 | 64 | 68 | 55.63 |

## P1 · costos y requests de org_53lc58dr entre 2025-08-01 y 2025-08-14

Consulta: `cql/02_queries.cql`, P1. 64 filas.

| org_id | usage_date | service | daily_cost_usd | negative_cost_usd | requests | event_count |
|---|---|---|---|---|---|---|
| org_53lc58dr | 2025-08-14 | analytics | 22.3154 | 0.0 | 344.0 | 3 |
| org_53lc58dr | 2025-08-14 | compute | 2.4244 | 0.0 | 0.0 | 3 |
| org_53lc58dr | 2025-08-14 | database | 30.5159 | 0.0 | 600.0 | 6 |
| org_53lc58dr | 2025-08-14 | genai | 44.1082 | 0.0 | 339.0 | 6 |
| org_53lc58dr | 2025-08-14 | networking | 0.0 | -1.1414 | 121.0 | 3 |
| org_53lc58dr | 2025-08-14 | storage | 2.9711 | 0.0 | 131.0 | 3 |
| org_53lc58dr | 2025-08-13 | analytics | 24.1012 | 0.0 | 319.0 | 3 |
| org_53lc58dr | 2025-08-13 | compute | 15.9698 | 0.0 | 202.0 | 3 |
| org_53lc58dr | 2025-08-13 | database | 32.8499 | 0.0 | 612.0 | 6 |
| org_53lc58dr | 2025-08-13 | genai | 21.8835 | 0.0 | 128.0 | 3 |
| org_53lc58dr | 2025-08-12 | analytics | 25.0617 | 0.0 | 366.0 | 6 |
| org_53lc58dr | 2025-08-12 | compute | 13.38 | 0.0 | 125.0 | 6 |
| org_53lc58dr | 2025-08-12 | database | 13.3276 | 0.0 | 239.0 | 3 |
| org_53lc58dr | 2025-08-12 | storage | 1.7468 | 0.0 | 116.0 | 3 |
| org_53lc58dr | 2025-08-11 | analytics | 8.8409 | 0.0 | 111.0 | 6 |
| org_53lc58dr | 2025-08-11 | compute | 15.1036 | 0.0 | 235.0 | 3 |
| org_53lc58dr | 2025-08-11 | database | 18.5643 | 0.0 | 361.0 | 6 |
| org_53lc58dr | 2025-08-11 | genai | 1.0253 | 0.0 | 0.0 | 3 |
| org_53lc58dr | 2025-08-11 | networking | 2.7014 | 0.0 | 258.0 | 3 |
| org_53lc58dr | 2025-08-10 | analytics | 28.0826 | 0.0 | 383.0 | 3 |
| org_53lc58dr | 2025-08-10 | compute | 19.4371 | 0.0 | 120.0 | 6 |
| org_53lc58dr | 2025-08-10 | database | 19.9626 | 0.0 | 354.0 | 6 |
| org_53lc58dr | 2025-08-10 | genai | 32.6456 | 0.0 | 260.0 | 3 |
| org_53lc58dr | 2025-08-10 | networking | 2.2711 | 0.0 | 223.0 | 3 |
| org_53lc58dr | 2025-08-09 | analytics | 17.32 | 0.0 | 251.0 | 3 |
| org_53lc58dr | 2025-08-09 | compute | 20.6426 | 0.0 | 245.0 | 6 |
| org_53lc58dr | 2025-08-09 | database | 1.0802 | 0.0 | 0.0 | 3 |
| org_53lc58dr | 2025-08-09 | genai | 8.545 | 0.0 | 0.0 | 3 |
| org_53lc58dr | 2025-08-09 | networking | 0.9785 | 0.0 | 121.0 | 3 |
| org_53lc58dr | 2025-08-08 | analytics | 27.2734 | 0.0 | 333.0 | 6 |
| org_53lc58dr | 2025-08-08 | compute | 16.256 | 0.0 | 132.0 | 3 |
| org_53lc58dr | 2025-08-08 | database | 12.5208 | 0.0 | 250.0 | 6 |
| org_53lc58dr | 2025-08-08 | genai | 12.7884 | -0.1854 | 115.0 | 3 |
| org_53lc58dr | 2025-08-08 | networking | 2.3145 | 0.0 | 241.0 | 3 |
| org_53lc58dr | 2025-08-08 | storage | 0.4204 | 0.0 | 0.0 | 3 |
| org_53lc58dr | 2025-08-07 | analytics | 22.0545 | 0.0 | 237.0 | 6 |
| org_53lc58dr | 2025-08-07 | compute | 9.8249 | 0.0 | 103.0 | 3 |
| org_53lc58dr | 2025-08-07 | database | 6.3905 | 0.0 | 110.0 | 6 |
| org_53lc58dr | 2025-08-07 | storage | 5.7852 | 0.0 | 255.0 | 3 |
| org_53lc58dr | 2025-08-06 | analytics | 13.7353 | 0.0 | 220.0 | 3 |
| org_53lc58dr | 2025-08-06 | compute | 29.0817 | 0.0 | 251.0 | 6 |
| org_53lc58dr | 2025-08-06 | genai | 15.9694 | 0.0 | 112.0 | 3 |
| org_53lc58dr | 2025-08-06 | networking | 0.2329 | 0.0 | 0.0 | 3 |
| org_53lc58dr | 2025-08-05 | analytics | 8.4587 | 0.0 | 112.0 | 3 |
| org_53lc58dr | 2025-08-05 | compute | 37.2031 | 0.0 | 474.0 | 6 |
| org_53lc58dr | 2025-08-05 | database | 31.7724 | 0.0 | 574.0 | 6 |
| org_53lc58dr | 2025-08-05 | networking | 0.0673 | 0.0 | 0.0 | 3 |
| org_53lc58dr | 2025-08-05 | storage | 5.4904 | 0.0 | 236.0 | 3 |
| org_53lc58dr | 2025-08-04 | analytics | 30.009 | 0.0 | 382.0 | 6 |
| org_53lc58dr | 2025-08-04 | compute | 8.2714 | 0.0 | 115.0 | 3 |
| org_53lc58dr | 2025-08-04 | database | 25.4942 | 0.0 | 496.0 | 6 |
| org_53lc58dr | 2025-08-04 | genai | 45.2335 | 0.0 | 349.0 | 3 |
| org_53lc58dr | 2025-08-03 | analytics | 25.4613 | 0.0 | 343.0 | 6 |
| org_53lc58dr | 2025-08-03 | compute | 12.6292 | 0.0 | 136.0 | 3 |
| org_53lc58dr | 2025-08-03 | genai | 19.2865 | 0.0 | 124.0 | 6 |
| org_53lc58dr | 2025-08-03 | networking | 1.402 | 0.0 | 114.0 | 3 |
| org_53lc58dr | 2025-08-03 | storage | 2.39 | 0.0 | 109.0 | 3 |
| org_53lc58dr | 2025-08-02 | analytics | 32.7553 | 0.0 | 478.0 | 6 |
| org_53lc58dr | 2025-08-02 | database | 1.5619 | 0.0 | 0.0 | 3 |
| org_53lc58dr | 2025-08-02 | networking | 2.2607 | 0.0 | 237.0 | 3 |
| org_53lc58dr | 2025-08-02 | storage | 4.8398 | 0.0 | 255.0 | 3 |
| org_53lc58dr | 2025-08-01 | database | 1.5839 | 0.0 | 0.0 | 6 |
| org_53lc58dr | 2025-08-01 | genai | 15.6442 | 0.0 | 131.0 | 3 |
| org_53lc58dr | 2025-08-01 | networking | 4.3094 | 0.0 | 375.0 | 3 |

## P2 · servicios con más costo de org_53lc58dr entre 2025-08-18 y 2025-08-31

Consulta: `cql/02_queries.cql`, P2. 68 filas leídas, sumadas por servicio en el cliente.

| service | cost_usd | dias_con_uso |
|---|---|---|
| analytics | 282.0746 | 13 |
| genai | 271.2743 | 11 |
| compute | 237.477 | 12 |
| database | 182.7793 | 14 |
| storage | 36.266 | 11 |
| networking | 15.4267 | 7 |
