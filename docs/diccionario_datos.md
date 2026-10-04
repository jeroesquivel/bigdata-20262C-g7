# Diccionario de datos — borrador v1 (Landing → Bronze)

**Alcance:** las fuentes de Landing y su tipado en Bronze. Las tablas Silver y Gold se agregan en la entrega 2.
**Cifras:** surgen de `notebooks/00_exploracion.ipynb` (ver `evidence/entrega1/perfil_landing.json`). PII = dato personal o sensible.
**Columnas técnicas:** todas las tablas Bronze agregan `ingest_ts` (timestamp), `source_file` (string) e `ingest_date` (date, partición).

## `customers_orgs` · 80 filas · PK `org_id`

| Columna | Tipo Bronze | Descripción | Nulos | Observaciones |
|---|---|---|---:|---|
| org_id | string | Identificador de la organización (tenant) | 0 | PK; clave de join con todas las fuentes |
| org_name | string | Nombre comercial | 0 | |
| industry | string | Industria | 0 | 10 valores |
| hq_region | string | Región de la sede central | 0 | 7 regiones |
| plan_tier | string | Plan contratado | 0 | free, standard, pro, enterprise |
| is_enterprise | boolean | Cliente enterprise | 0 | Llega como `True`/`False` |
| signup_date | date | Fecha de alta | 0 | 2025-05-04 → 2025-07-02 |
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
| last_login | date | Último acceso | 139 | `null` = nunca ingresó o sin dato |

## `resources` · 400 filas · PK `resource_id`

| Columna | Tipo Bronze | Descripción | Nulos | Observaciones |
|---|---|---|---:|---|
| resource_id | string | Identificador del recurso cloud | 0 | PK; FK desde los eventos |
| org_id | string | Organización propietaria | 0 | FK → customers_orgs |
| service | string | Servicio | 0 | compute, storage, database, networking, analytics, genai |
| region | string | Región | 0 | 7 regiones |
| created_at | date | Fecha de creación | 0 | |
| state | string | Estado | 0 | running, stopped, terminated |
| tags_json | string | Etiquetas (arreglo JSON serializado) | 83 | Algunas incluyen `pii:true` (**sensible**). Se parsea en Silver |

## `support_tickets` · 1.000 filas · PK `ticket_id`

| Columna | Tipo Bronze | Descripción | Nulos | Observaciones |
|---|---|---|---:|---|
| ticket_id | string | Identificador del ticket | 0 | PK |
| org_id | string | Organización | 0 | FK → customers_orgs |
| category | string | Categoría | 0 | 6 valores |
| severity | string | Severidad | 0 | low 412, medium 328, high 204, critical 56 |
| created_at | date | Fecha de apertura | 0 | 2025-05-09 → 2025-08-31 |
| resolved_at | date | Fecha de resolución | 240 | `null` = ticket abierto (estado válido) |
| csat | double | Satisfacción del cliente (escala supuesta 1–5) | 254 | 40 fuera de rango (0, 6, 7) → `null` + flag (R10) |
| sla_breached | boolean | Se incumplió el SLA | 0 | |

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
| subtotal | double | Importe antes de impuestos, en moneda original | 0 | |
| credits | double | Créditos o ajustes (positivos, se restan) | 137 | `null` → 0 en Silver |
| taxes | double | Impuestos, en moneda original | 0 | |
| currency | string | Moneda | 0 | USD 160, ARS 51, EUR 29 |
| exchange_rate_to_usd | double | Tasa de conversión a USD | 0 | Las facturas USD tienen tasa ≠ 1 (0,855–1,118) → flag `fx_suspect` (A3) |

## `usage_events` (de `usage_events_stream/*.jsonl`) · 43.200 filas · PK `event_id`

| Columna | Tipo Bronze | Descripción | Nulos | Observaciones |
|---|---|---|---:|---|
| event_id | string | Identificador del evento | 0 | PK; 0 duplicados en la muestra |
| timestamp | timestamp (UTC) | Momento del evento | 0 | 2025-07-03 → 2025-08-31; en Silver se deriva `event_date` |
| org_id | string | Organización | 0 | FK → customers_orgs |
| resource_id | string | Recurso | 0 | FK → resources (el `service` siempre coincide) |
| service | string | Servicio | 0 | |
| region | string | Región | 0 | |
| metric | string | Métrica medida | 0 | requests, cpu_hours, storage_gb_hours |
| value | **string** | Valor de la métrica | 877 | Llega mezclado (1.309 como texto). Se castea a double en Silver (D13) |
| unit | string | Unidad | 2.075 | Se imputa desde `metric` (D15) |
| cost_usd_increment | double | Costo incremental en USD | 0 | 216 negativos; flag si < −0,01 (R5) |
| schema_version | int | Versión del esquema | 0 | v1: 10.800 · v2: 32.400 (desde 2025-07-18) |
| carbon_kg | double | Emisiones estimadas (kg CO₂) | 10.800 | Solo en v2; llega como entero o decimal |
| genai_tokens | long | Tokens GenAI consumidos | 40.068 | Solo en 3.132 eventos `genai` v2 |
