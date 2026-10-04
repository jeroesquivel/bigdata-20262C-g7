# Diccionario de datos — borrador v1 (Landing → Bronze)

**Alcance:** las fuentes de Landing y su tipado en Bronze. Las tablas Silver y Gold se agregan en la entrega 2.
**Cifras:** surgen de `notebooks/00_exploracion.ipynb` (ver `evidence/entrega1/perfil_landing.json`); las de consistencia entre fuentes, de sus §2.3 (maestros y facturación) y §3.1 (eventos contra recursos). Las reglas (R1–R15) están en la §6.5 del documento de diseño. PII = dato personal o sensible.
**Columnas técnicas:** todas las tablas Bronze agregan `ingest_ts` (timestamp), `source_file` (string) e `ingest_date` (date, partición).

## `customers_orgs` · 80 filas · PK `org_id`

| Columna | Tipo Bronze | Descripción | Nulos | Observaciones |
|---|---|---|---:|---|
| org_id | string | Identificador de la organización (tenant) | 0 | PK; clave de join con todas las fuentes |
| org_name | string | Nombre comercial | 0 | |
| industry | string | Industria | 0 | 10 valores |
| hq_region | string | Región de la sede central | 0 | 7 regiones |
| plan_tier | string | Plan contratado | 0 | free, standard, pro, enterprise. Fuente de verdad del segmento enterprise (R15, A7) |
| is_enterprise | boolean | Cliente enterprise | 0 | Llega como `True`/`False`. Contradice `plan_tier` en 25 orgs (9 `enterprise` con `False`; 16 con `True` y plan standard 8, pro 6, free 2) → en Silver se deriva de `plan_tier` + flag `enterprise_flag_mismatch` (R15) |
| signup_date | date | Fecha de alta | 0 | 2025-05-04 → 2025-07-02. 209 tickets tienen `created_at` anterior (R13) |
| sales_rep | string | Ejecutivo comercial | 0 | 5 valores |
| lifecycle_stage | string | Etapa del ciclo de vida | 0 | lead, prospect, active, at_risk, churned |
| marketing_source | string | Canal de adquisición | 0 | |
| nps_score | double | NPS de la organización (−100..100) | 11 | 1 valor fuera de rango (101) → `null` + flag (R10) |

## `users` · 800 filas · PK `user_id`

| Columna | Tipo Bronze | Descripción | Nulos | Observaciones |
|---|---|---|---:|---|
| user_id | string | Identificador del usuario | 0 | PK |
| org_id | string | Organización | 0 | FK → customers_orgs |
| email | string | Email del usuario | 0 | **PII**: no se propaga a Gold ni a serving |
| role | string | Rol | 0 | 6 valores |
| active | boolean | Usuario activo | 0 | |
| created_at | date | Fecha de alta | 0 | |
| last_login | date | Último acceso | 139 | `null` = nunca ingresó o sin dato. 232 anteriores a `created_at` → flag `is_login_before_created` (R13) |

## `resources` · 400 filas · PK `resource_id`

| Columna | Tipo Bronze | Descripción | Nulos | Observaciones |
|---|---|---|---:|---|
| resource_id | string | Identificador del recurso cloud | 0 | PK; FK desde los eventos |
| org_id | string | Organización propietaria | 0 | FK → customers_orgs |
| service | string | Servicio | 0 | compute, storage, database, networking, analytics, genai |
| region | string | Región | 0 | 7 regiones; coincide siempre con la de sus eventos |
| created_at | date | Fecha de creación | 0 | 7.371 eventos (17 %) tienen `timestamp` anterior → flag `is_before_resource_created` en los eventos (R13) |
| state | string | Estado | 0 | running, stopped, terminated. Estado actual (snapshot): 4.167 eventos son de recursos hoy `terminated`, lo que es válido |
| tags_json | string | Etiquetas (arreglo JSON serializado) | 83 | Algunas incluyen `pii:true` (**sensible**). Se parsea en Silver |

## `support_tickets` · 1.000 filas · PK `ticket_id`

| Columna | Tipo Bronze | Descripción | Nulos | Observaciones |
|---|---|---|---:|---|
| ticket_id | string | Identificador del ticket | 0 | PK |
| org_id | string | Organización | 0 | FK → customers_orgs |
| category | string | Categoría | 0 | 6 valores |
| severity | string | Severidad | 0 | low 412, medium 328, high 204, critical 56 |
| created_at | date | Fecha de apertura | 0 | 2025-05-09 → 2025-08-31. 209 anteriores al `signup_date` de la org → flag `is_before_org_signup` (R13) |
| resolved_at | date | Fecha de resolución | 240 | `null` = ticket abierto (estado válido) |
| csat | double | Satisfacción del cliente (escala supuesta 1–5) | 254 | 40 fuera de rango (0, 6, 7) → `null` + flag (R10). 172 de los 240 tickets abiertos tienen CSAT (se mide al cerrar) → flag `csat_on_open_ticket` y se excluyen del promedio (R14) |
| sla_breached | boolean | Se incumplió el SLA | 0 | 25 tickets abiertos con `True` (válido: el SLA ya venció) |

## `marketing_touches` · 1.500 filas · PK `touch_id`

| Columna | Tipo Bronze | Descripción | Nulos | Observaciones |
|---|---|---|---:|---|
| touch_id | string | Identificador de la interacción | 0 | PK |
| org_id | string | Organización | 0 | FK → customers_orgs |
| campaign | string | Campaña | 0 | 6 valores |
| channel | string | Canal | 0 | ads, email, in_app, event |
| timestamp | date | Fecha de la interacción | 0 | Solo fecha (sin hora) |
| clicked | boolean | Hizo click | 0 | |
| converted | boolean | Convirtió | 0 | Fuente fuera de los marts obligatorios |

## `nps_surveys` · 92 filas · PK (`org_id`, `survey_date`)

| Columna | Tipo Bronze | Descripción | Nulos | Observaciones |
|---|---|---|---:|---|
| org_id | string | Organización | 0 | FK → customers_orgs; 60 organizaciones |
| survey_date | date | Fecha de la encuesta | 0 | 2025-05-24 → 2025-08-31 |
| nps_score | double | NPS (−100..100) | 19 | Sin valores fuera de rango |
| comment | string | Comentario categórico | 10 | 6 valores |

## `billing_monthly` · 240 filas · PK `invoice_id` (única también por `org_id`, `month`)

| Columna | Tipo Bronze | Descripción | Nulos | Observaciones |
|---|---|---|---:|---|
| invoice_id | string | Identificador de la factura | 0 | PK |
| org_id | string | Organización | 0 | FK → customers_orgs |
| month | date | Mes facturado (primer día) | 0 | 2025-06-01, 2025-07-01, 2025-08-01 |
| subtotal | double | Importe antes de impuestos, en moneda original | 0 | 13 negativos (mín. −1.671,83; 10 USD, 2 ARS, 1 EUR) → nota de crédito, flag `is_credit_note` (R11, A6) |
| credits | double | Créditos o ajustes (positivos, se restan) | 137 | `null` → 0 en Silver. 9 facturas con `credits > subtotal`, todas notas de crédito → flag `credits_exceed_subtotal` (R12) |
| taxes | double | Impuestos, en moneda original | 0 | Siempre 21 % del valor absoluto de `subtotal`, así que en las 13 notas de crédito llega positivo → en Silver toma el signo del subtotal (R11) |
| currency | string | Moneda | 0 | USD 160, ARS 51, EUR 29 |
| exchange_rate_to_usd | double | Tasa de conversión a USD | 0 | Las facturas USD tienen tasa ≠ 1 (0,855–1,118) → flag `fx_suspect` (A3) |

## `usage_events` (de `usage_events_stream/*.jsonl`) · 43.200 filas · PK `event_id`

| Columna | Tipo Bronze | Descripción | Nulos | Observaciones |
|---|---|---|---:|---|
| event_id | string | Identificador del evento | 0 | PK; 0 duplicados en la muestra |
| timestamp | timestamp (UTC) | Momento del evento | 0 | 2025-07-03 → 2025-08-31; en Silver se deriva `event_date`. Debe ser ≤ `ingest_ts` + 5 min (R3). 7.371 (17 %) anteriores al `created_at` del recurso → flag `is_before_resource_created` (R13) |
| org_id | string | Organización | 0 | FK → customers_orgs; coincide siempre con la org del recurso |
| resource_id | string | Recurso | 0 | FK → resources (`service`, `org_id` y `region` siempre coinciden) |
| service | string | Servicio | 0 | |
| region | string | Región | 0 | Coincide siempre con la del recurso |
| metric | string | Métrica medida | 0 | requests, cpu_hours, storage_gb_hours |
| value | **string** | Valor de la métrica | 877 | Llega mezclado (1.309 como texto). Se castea a double en Silver (D13) |
| unit | string | Unidad | 2.075 | Se imputa desde `metric` (D15) |
| cost_usd_increment | double | Costo incremental en USD | 0 | 216 negativos; flag `is_negative_cost` si < −0,01 (R5). Base del costo diario para el MAD (`is_cost_anomaly`, D11) |
| schema_version | int | Versión del esquema | 0 | v1: 10.800 · v2: 32.400 (desde 2025-07-18) |
| carbon_kg | double | Emisiones estimadas (kg CO₂) | 10.800 | Solo en v2; llega como entero o decimal |
| genai_tokens | long | Tokens GenAI consumidos | 40.068 | Solo en 3.132 eventos `genai` v2 |
