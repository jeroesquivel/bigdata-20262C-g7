# Lote de prueba con errores inyectados (D23)

Los datos reales de Landing no tienen `event_id` nulos ni duplicados, líneas ilegibles ni claves huérfanas, así que la quarantine queda vacía. Este lote, que **no** está en `datalake/landing/`, tiene un caso por regla para mostrar que el pipeline las aplica. Lo usan la sección 4.2 del notebook `01_pipeline_entrega2.ipynb` y las pruebas de `tests/`.

Se procesa con `config_fixture.yaml`, que es igual a la configuración del proyecto salvo dos rutas: Landing es `tests/fixtures/landing/` y el lago es `datalake_fixture/` (no versionado). Para borrarlo: `python scripts/reset_lake.py --config tests/fixtures/config_fixture.yaml --si`.

## Maestros

| Archivo | Fila | Qué prueba | Resultado esperado |
|---|---|---|---|
| `customers_orgs.csv` | `org_fx1`, `org_fx2` | Organizaciones válidas | Bronze |
| `customers_orgs.csv` | `org_fx3` | `signup_date` no es una fecha | Quarantine de Bronze con `parse_error` |
| `resources.csv` | `res_fx1` | Recurso válido, creado el 2025-01-15 | Bronze |
| `resources.csv` | `res_fx2` | Recurso creado el 2025-09-01, después de su evento | Bronze (su evento lleva el flag R13) |

## Eventos

| Archivo | `event_id` | Qué prueba | Resultado esperado |
|---|---|---|---|
| `events_fx_0001.jsonl` | `fx_ok_1` | Evento v2 válido | Silver |
| | `fx_ok_v1` | Evento v1, sin `carbon_kg` | Silver con `carbon_kg` nulo (D14) |
| | `fx_neg_cost` | Costo -5,0 | Silver con `is_negative_cost` (R5) |
| | `fx_unit_null` | `value` sin `unit`, métrica `cpu_hours` | Silver con `unit = hours` y `unit_imputed` (R6) |
| | `fx_value_text` | `value = "abc"` | Silver con `value_num` nulo y `value_cast_failed` (R7) |
| | (sin id) | Falta `event_id` | Quarantine de Silver, `R1_missing_event_id` |
| | `fx_no_timestamp` | Falta `timestamp` | Quarantine, `R3_invalid_timestamp` |
| | `fx_future` | `timestamp` en 2099 | Quarantine, `R3_future_timestamp` |
| | `fx_orphan_org` | `org_id` inexistente | Quarantine, `R4_orphan_org_id` |
| | `fx_orphan_res` | `resource_id` inexistente | Quarantine, `R4_orphan_resource_id` |
| | `fx_unknown_metric` | Métrica `gpu_hours` sin `unit` | Quarantine, `R6_unknown_metric_unit` |
| | `fx_cut` | Línea JSON cortada | Quarantine de Bronze con `parse_error` |
| `events_fx_0002.jsonl` | `fx_ok_1` | Duplicado del primer archivo | Lo descarta la deduplicación del stream (D5) |
| | `fx_before_created` | Evento anterior al alta de `res_fx2` | Silver con `is_before_resource_created` (R13) |

Totales: el stream lee 14 líneas y escribe 12 en Bronze (1 ilegible y 1 duplicado). Silver recibe 12: 6 válidas y 6 a quarantine. El duplicado no llega a Silver porque lo descarta el stream; la deduplicación de Silver (R2) es la segunda protección y se prueba aparte en `tests/`.
