# Silver y calidad, corrida 20261114T002224Z

## Conservación

| rows_read | rows_written | rows_rejected | rows_duplicated | conserva |
|---|---|---|---|---|
| 43200 | 43200 | 0 | 0 | True |

## Reporte por regla

| regla | tipo | accion | casos |
|---|---|---|---|
| R1_missing_event_id | bloqueante | quarantine | 0 |
| R3_invalid_timestamp | bloqueante | quarantine | 0 |
| R3_future_timestamp | bloqueante | quarantine | 0 |
| R4_orphan_org_id | bloqueante | quarantine | 0 |
| R4_orphan_resource_id | bloqueante | quarantine | 0 |
| R6_unknown_metric_unit | bloqueante | quarantine | 0 |
| R2_duplicate_event_id | deduplicación | se conserva una copia | 0 |
| R5_is_negative_cost | flag | flag; va aparte en Gold | 211 |
| R6_unit_imputed | flag | se completa desde metric | 2038 |
| R7_value_cast_failed | flag | value_num nulo | 0 |
| R13_is_before_resource_created | flag | flag | 7371 |

## Silver usage_events

60 particiones por event_date, 60 archivos, 2896429 bytes en `datalake/silver/usage_events`. dim_org: 80 filas; dim_resource: 400 filas.
