# Muestras de quarantine (lote de prueba, D23), corrida 20261114T002243Z

Generadas con `tests/fixtures/config_fixture.yaml` sobre `tests/fixtures/landing/`, no con los datos reales.

## Conteos

| etapa | leidas | escritas | quarantine | duplicadas |
|---|---|---|---|---|
| bronze_batch.customers_orgs | 3 | 2 | 1 | 0 |
| bronze_batch.resources | 2 | 2 | 0 | 0 |
| bronze_stream | 14 | 12 | 1 | 1 |
| silver.usage_events | 12 | 6 | 6 | 0 |

## Quarantine de Silver (reglas bloqueantes)

| event_id | org_id | resource_id | metric | timestamp | dq_errors | source_file |
|---|---|---|---|---|---|---|
|  | org_fx1 | res_fx1 | requests | 2025-08-01 10:00 | R1_missing_event_id | usage_events_stream/events_fx_0001.jsonl |
| fx_future | org_fx1 | res_fx1 | requests | 2099-01-01 00:00 | R3_future_timestamp | usage_events_stream/events_fx_0001.jsonl |
| fx_no_timestamp | org_fx1 | res_fx1 | requests |  | R3_invalid_timestamp | usage_events_stream/events_fx_0001.jsonl |
| fx_orphan_org | org_missing | res_fx1 | requests | 2025-08-01 10:00 | R4_orphan_org_id | usage_events_stream/events_fx_0001.jsonl |
| fx_orphan_res | org_fx1 | res_missing | requests | 2025-08-01 10:00 | R4_orphan_resource_id | usage_events_stream/events_fx_0001.jsonl |
| fx_unknown_metric | org_fx1 | res_fx1 | gpu_hours | 2025-08-01 10:00 | R6_unknown_metric_unit | usage_events_stream/events_fx_0001.jsonl |

## Quarantine de Bronze (registros ilegibles)

| source_file | raw_record | dq_errors |
|---|---|---|
| usage_events_stream/events_fx_0001.jsonl | {"event_id": "fx_cut", "timestamp": "2025-08-01T10: | parse_error |
| customers_orgs.csv | org_fx3,Fixture Tres,Retail,us-east,free,False,no-es-una-fecha,rep_c,l | parse_error |

## Reporte por regla

| regla | tipo | accion | casos |
|---|---|---|---|
| R1_missing_event_id | bloqueante | quarantine | 1 |
| R3_invalid_timestamp | bloqueante | quarantine | 1 |
| R3_future_timestamp | bloqueante | quarantine | 1 |
| R4_orphan_org_id | bloqueante | quarantine | 1 |
| R4_orphan_resource_id | bloqueante | quarantine | 1 |
| R6_unknown_metric_unit | bloqueante | quarantine | 1 |
| R2_duplicate_event_id | deduplicación | se conserva una copia | 0 |
| R5_is_negative_cost | flag | flag; va aparte en Gold | 1 |
| R6_unit_imputed | flag | se completa desde metric | 1 |
| R7_value_cast_failed | flag | value_num nulo | 1 |
| R13_is_before_resource_created | flag | flag | 1 |
