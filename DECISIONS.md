# Registro de decisiones

Formato: decisión tomada, alternativas descartadas y justificación. Estado posible: **Vigente**, **A validar** (requiere evidencia en la entrega 2) o **Reemplazada**. Las secciones citadas (§) son del [documento de diseño](docs/diseno_entrega1.md).

| # | Decisión | Alternativas descartadas | Justificación | Estado |
|---|---|---|---|---|
| D1 | **Patrón Lambda simplificada:** streaming solo para la ingesta de eventos, batch para maestros y facturación, y Silver/Gold en batch sobre Bronze | Kappa · Lambda clásica con vistas de velocidad y batch separadas · Batch puro · Streaming hasta Gold | Coincide con la naturaleza de las fuentes, cumple el requisito invariable (§4.3 de la consigna) y evita duplicar lógica: hay un solo motor y una sola implementación (§5) | Vigente |
| D2 | **PySpark 3.5.3 en Google Colab** (`local[*]`) | Spark 4.x · Databricks Community · Dataproc/EMR · pandas | Colab es el entorno que valida la consigna y no tiene costo. Spark 3.5 tiene soporte estable de spark-cassandra-connector 3.5 (Scala 2.12). pandas no cumple la consigna. Un cluster administrado agrega costo sin beneficio para 12,6 MB | Vigente |
| D3 | **Parquet (snappy)** en Google Drive | Delta Lake / Iceberg · GCS/S3 · HDFS | La consigna exige Parquet. Delta aportaría MERGE y ACID, pero suma una dependencia y una curva de aprendizaje que no hacen falta: la idempotencia se resuelve con D7. Un bucket requiere gestionar credenciales y costos | Vigente |
| D4 | **File source de Structured Streaming** con `maxFilesPerTrigger=10` y `trigger(availableNow=True)` | Kafka / Redpanda · script que copie archivos progresivamente · trigger continuo | La fuente ya son archivos. Kafka agrega infraestructura sin que exista un productor real. `availableNow` vuelve la corrida determinística y reproducible (12 micro-lotes) | Vigente |
| D5 | **Dedupe en streaming con watermark sobre `ingest_ts`** (`dropDuplicatesWithinWatermark(["event_id"])`, 1 h). Los eventos tardíos según *event-time* se absorben en el recálculo batch de Silver | Watermark sobre `timestamp` (event-time) · `dropDuplicates` sin watermark | Cada archivo abarca los 60 días: con event-time se descartaría el 91,6 % (1 h), el 90,1 % (1 día), el 81,0 % (7 días) o el 45,7 % (30 días) de los eventos (`notebooks/00_exploracion.ipynb`, §4). Sin watermark, el estado crece sin límite. **Fallback:** dedupe sin estado dentro de cada micro-lote + unicidad global en Silver (R2) | A validar |
| D6 | **Particionado:** Bronze por `ingest_date`; Silver de eventos por `event_date`; maestros y Gold sin particionar. Silver toma el último snapshot (`ingest_date`) de cada maestro | Por `service`, `org_id` u hora · Bronze por fecha de evento | La fecha es el filtro de todas las consultas. Con 720 eventos por día, subdividir más produce archivos diminutos. Si Bronze se particionara por fecha de evento, cada micro-lote escribiría en 60 particiones (§6.1) | Vigente |
| D7 | **Idempotencia:** checkpoint + dedupe por clave natural (la unicidad global en Silver cubre también los reintentos *at-least-once* de `foreachBatch`) + overwrite dinámico por partición en el Bronze batch + upsert por clave primaria en Cassandra | MERGE (Delta) · tabla de control de cargas | Es lo mínimo que garantiza los criterios CE2 y CE3 sin dependencias extra | Vigente |
| D8 | **Reglas de calidad como funciones PySpark propias**, que agregan una columna `dq_errors`, con quarantine en Parquet | Great Expectations · Deequ · Soda | Son 10 reglas simples. Una librería agrega configuración y dependencias sin mejorar la evidencia evaluable | Vigente |
| D9 | **AstraDB (free tier)** con spark-cassandra-connector. Plan B: `cassandra-driver` de Python | Cassandra local en Docker | Docker no corre en Colab y AstraDB es lo que nombra la consigna. La consigna admite un driver como mecanismo de carga | A validar |
| D10 | **Orquestación con un notebook o script secuencial** (`run_pipeline`) | Airflow · Prefect · Dagster | Son 5 etapas lineales; un orquestador sería over-engineering | Vigente |
| D11 | **Anomalías con robust z-score (MAD)** por `(org_id, service)`; umbral inicial de 3,5 | z-score clásico · percentiles fijos · Isolation Forest · Prophet | Los costos tienen spikes (p50 = 1,00; máximo 317,43) que distorsionan la media y el desvío estándar. MAD es robusto, explicable y no requiere entrenar un modelo | Vigente (el umbral se ajusta en la entrega final) |
| D12 | **`dim_org` y `dim_resource` como SCD Tipo 1** | SCD Tipo 2 | Hay un único snapshot sin cambios para historizar. Los snapshots de Bronze por `ingest_date` conservan la historia | Vigente |
| D13 | **`value` se lee como string en Bronze** y se castea en Silver con fallback | Leerlo como double desde el origen | Llega con tipos mezclados (41.014 numéricos, 1.309 texto, 877 null). Castearlo en Silver deja rastro con un flag | Vigente |
| D14 | **Columnas v2 en `null` para los eventos v1**, no en 0 | Rellenar con 0 | Hay que distinguir "no medido" de "cero" en `carbon_kg` y `genai_tokens` | Vigente |
| D15 | **`unit` faltante se imputa desde `metric`** (+ flag `unit_imputed`), en lugar de mandar el registro a quarantine | Quarantine | Cada métrica tiene una única unidad en el 100 % de los casos observados. Mandar 2.038 eventos a quarantine (4,7 %) perdería costo real | Vigente |
| D16 | **Diagramas en Mermaid** dentro del Markdown | draw.io · herramientas pagas | Quedan versionados junto al código y GitHub los renderiza | Vigente |

## Decisiones abiertas

| # | Pregunta | Propuesta |
|---|---|---|
| A1 | ¿Se acepta que Gold se actualice por corrida batch, o se espera streaming hasta Gold? | Gold por corrida batch |
| A2 | ¿Los costos < −0,01 se suman en `daily_cost_usd`? | Se excluyen de la suma y se exponen en `negative_cost_usd` y `anomalous_events` |
| A3 | Las facturas en USD con tasa ≠ 1 (160 de 160), ¿se fuerzan a 1,0? | Forzar 1,0 y marcar con `fx_suspect` |
| A4 | El top-N de la consulta P2, ¿se resuelve en el cliente o con una tabla precalculada? | En el cliente (≤ 84 filas por organización) |
| A5 | La consigna no aclara si P3, P4 y P5 son globales o por organización | P3 global por día; P4 y P5 por organización, como el grano de los marts de referencia |
