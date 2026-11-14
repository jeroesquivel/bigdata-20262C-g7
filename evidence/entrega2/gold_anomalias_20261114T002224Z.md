# Gold, corrida 20261114T002224Z

## org_daily_usage_by_service

| rows | distinct_keys | grain_unique | gold_cost_total | silver_cost_total | cost_diff | cost_reconciles | gold_negative_events | silver_negative_events | gold_event_count | silver_events |
|---|---|---|---|---|---|---|---|---|---|---|
| 11050 | 11050 | True | 147433.9778 | 147433.9778 | 0.0 | True | 211 | 211 | 43200 | 43200 |

1 archivo(s), 298055 bytes en `datalake/gold/org_daily_usage_by_service`.

Muestra del mart:

| org_id | usage_date | service | daily_cost_usd | negative_cost_usd | negative_cost_events | requests | cpu_hours | storage_gb_hours | genai_tokens | carbon_kg | event_count |
|---|---|---|---|---|---|---|---|---|---|---|---|
| org_0lvsnujz | 2025-07-03 | analytics | 8.8739 | 0.0 | 0 | 116.0 | 4.6168 | 12.7808 |  |  | 3 |
| org_0lvsnujz | 2025-07-03 | database | 0.5723 | 0.0 | 0 | 0.0 | 1.7515 | 14.7891 |  |  | 3 |
| org_0lvsnujz | 2025-07-03 | storage | 5.5542 | 0.0 | 0 | 255.0 | 0.0 | 4.6668 |  |  | 3 |
| org_0lvsnujz | 2025-07-04 | analytics | 3.2915 | 0.0 | 0 | 0.0 | 2.7659 | 38.5678 |  |  | 3 |
| org_0lvsnujz | 2025-07-04 | database | 12.7337 | 0.0 | 0 | 235.0 | 2.5139 | 41.2334 |  |  | 6 |
| org_0lvsnujz | 2025-07-04 | storage | 1.3341 | -2.8393 | 1 | 222.0 | 1.1055 | 0.0 |  |  | 3 |
| org_0lvsnujz | 2025-07-05 | analytics | 7.5042 | 0.0 | 0 | 127.0 | 1.9506 | 0.0 |  |  | 3 |
| org_0lvsnujz | 2025-07-05 | database | 6.6109 | 0.0 | 0 | 102.0 | 2.6635 | 23.1051 |  |  | 3 |

## Anomalías de costo (MAD)

| threshold | rows_written | anomalies | anomalies_pct | pairs | pairs_mad_zero |
|---|---|---|---|---|---|
| 3.5 | 11050 | 130 | 1.18 | 262 | 0 |

Sensibilidad al umbral:

| umbral | anomalias | pct_filas | pares_org_servicio |
|---|---|---|---|
| 2.5 | 332 | 3.0 | 147 |
| 3.5 | 130 | 1.18 | 91 |
| 5.0 | 83 | 0.75 | 69 |

Diez anomalías con mayor score:

| org_id | usage_date | service | daily_cost_usd | median_cost | mad_cost | anomaly_score |
|---|---|---|---|---|---|---|
| org_okep7y6w | 2025-07-03 | compute | 202.646 | 11.1426 | 2.7475 | 47.0133 |
| org_x7eedtjv | 2025-07-09 | compute | 311.255 | 13.8981 | 4.3827 | 45.7634 |
| org_okep7y6w | 2025-08-07 | compute | 191.8968 | 11.1426 | 2.7475 | 44.3744 |
| org_w3zp08j3 | 2025-07-05 | networking | 27.4203 | 1.4849 | 0.4557 | 38.388 |
| org_w3zp08j3 | 2025-07-16 | networking | 26.4157 | 1.4849 | 0.4557 | 36.9011 |
| org_x7eedtjv | 2025-08-23 | compute | 211.2481 | 13.8981 | 4.3827 | 30.3723 |
| org_874frhoc | 2025-07-17 | networking | 27.651 | 1.708 | 0.579 | 30.222 |
| org_pht0hl9x | 2025-08-25 | analytics | 257.042 | 14.6066 | 5.4627 | 29.9344 |
| org_d14ve92m | 2025-07-20 | analytics | 181.7988 | 10.70105 | 4.071 | 28.3482 |
| org_pnsm43d8 | 2025-07-17 | storage | 56.6704 | 3.58035 | 1.291 | 27.7376 |
