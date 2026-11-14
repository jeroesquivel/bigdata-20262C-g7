# Gobierno de datos (preliminar, entrega 2)

Cubre los activos que existen en esta entrega: qué son, quién responde por ellos, qué controles tienen y dónde está la evidencia. Los responsables salen de los roles del diseño (sección 10.4): **Agustín Ronda** (ingesta), **Pedro Salinas** (procesamiento y calidad) y **Jerónimo Esquivel** (serving, gobierno y documentación).

## Matriz por activo

| Activo | Responsable | Sensibilidad | Reglas de calidad | Métricas que se registran | Linaje | Acceso | Retención (sección 6.3) | Monitoreo y evidencia |
|---|---|---|---|---|---|---|---|---|
| Landing (`datalake/landing/`) | Agustín | Contiene PII: `users.email`; algunos recursos tienen la etiqueta `pii:true` | Ninguna: es el dato tal como llega | Filas por archivo (las cuenta `bronze_batch` como líneas de texto) | Origen de todo el lago | Solo lectura. `lake.write_parquet` y `reset_lake` se niegan a escribir o borrar dentro de Landing | Indefinida | Conservación Landing = Bronze + quarantine en `bronze_batch_*.md` |
| Bronze maestros (`bronze/<fuente>/ingest_date=D/`) | Agustín | Igual que Landing (incluye `email`) | Esquema explícito; fila ilegible → quarantine con `parse_error` | Leídas, escritas, rechazadas, archivos y bytes por fuente | `source_file`, `ingest_ts`, `ingest_date` | Equipo de datos | 90 días para snapshots; 13 meses para facturación | `bronze_batch_*.md`, run log |
| Bronze eventos (`bronze/usage_events/ingest_date=D/batch_id=N/`) | Agustín | Sin PII | Esquema explícito; ilegible → quarantine; deduplicación por `dedupe_key` con watermark (D5) | Por micro-lote: filas leídas, watermark, estado, descartes por watermark y duplicados | `source_file`, `ingest_ts`, `batch_id`, `dedupe_key` | Equipo de datos | 13 meses | `bronze_stream_*.md` (progreso, checkpoint, reinicio) |
| Checkpoint (`_checkpoints/bronze_usage_events/`) | Agustín | Sin datos de negocio | No se edita a mano; se borra solo junto con Bronze de eventos (`reset_lake.py`) | Archivos leídos, offsets, lotes confirmados, estado | Registra qué archivos de Landing ya entraron | Solo el pipeline | Mientras exista el stream | Contenido listado en `bronze_stream_*.md` |
| Silver `dim_org`, `dim_resource` | Pedro | `dim_resource.tags_json` puede marcar recursos con PII; no hay emails | Último snapshot y una fila por clave natural (SCD 1, D12) | Filas por tabla | Desde Bronze, con `snapshot_date` | Equipo de datos y analistas | 13 meses | `silver_calidad_*.md` |
| Silver `usage_events` (`silver/usage_events/event_date=D/`) | Pedro | Sin PII | R1, R3, R4, R6 (bloqueantes); R2 (dedupe); R5, R6, R7, R13 (flags) | Reporte por regla, conservación Bronze = Silver + quarantine + duplicados | Conserva `source_file`, `ingest_ts`, `ingest_date` y `batch_id` de Bronze | Equipo de datos y analistas | 13 meses | `silver_calidad_*.md`, `explain_silver_join_*.txt` |
| Quarantine (`quarantine/`) | Pedro | Puede contener cualquier dato del registro original | Registro original más `dq_errors` | Filas por regla | `source_file` del registro | Equipo de datos | 30 días | `quarantine_muestras_*.md` (lote de prueba, D23) |
| Gold `org_daily_usage_by_service` | Pedro | Sin PII; datos de costo por cliente (confidencial comercial) | Grano único; costo conciliado con Silver; negativos aparte (R5) | Filas, claves distintas, costo total, eventos negativos | `groupBy` sobre Silver (sección 8) | Analistas, FinOps, serving | 13 meses | `gold_anomalias_*.md`, `explain_gold_*.txt` |
| Gold `cost_anomaly_mart` | Pedro | Igual que el mart | MAD > 0 o referencia del servicio (D11) | Anomalías, porcentaje, pares, sensibilidad al umbral | Desde `org_daily_usage_by_service` | Analistas, FinOps | 13 meses | `gold_anomalias_*.md` |
| AstraDB `cpa.org_daily_usage_by_service` | Jerónimo | Igual que el mart | Upsert por clave primaria; conteo igual a Gold | Filas cargadas y leídas, filas de P1 contra Gold | Carga desde Gold en cada corrida | Token de aplicación del equipo | Se reescribe en cada carga | `serving_astradb_*.md`, capturas de P1 y P2 |
| Run log (`_meta/run_log/`) | Jerónimo | Sin datos de negocio | Un archivo por corrida que no se pisa, también si la corrida falla | Por etapa: leídas, escritas, rechazadas, duplicadas, duración, estado y error | Una fila por etapa de cada corrida, con el `run_id` | Equipo de datos | Indefinida | `run_log_*.jsonl` |

## Metadatos y linaje

- **Por fila.** Bronze agrega `ingest_ts`, `ingest_date` y `source_file` (ruta relativa a Landing, por ejemplo `usage_events_stream/events_part_0007.jsonl`). Los eventos suman `batch_id` y `dedupe_key`. Silver los conserva, así que cada evento del mart se puede rastrear hasta su archivo y su micro-lote.
- **Por corrida.** El run log guarda, para cada etapa, conteos, duración, estado y la configuración usada. De ahí salen los controles de conservación (CE1) y la duración de cada corrida (CE4).
- **De negocio.** El [diccionario de datos](diccionario_datos.md) describe las columnas de cada zona y su regla de calidad.

## Secretos

- El token de AstraDB y el *secure connect bundle* no se versionan. `.gitignore` excluye `secure-connect*.zip`, `config/config.yaml` y los `.env`.
- En Colab, el token se guarda en Colab Secrets (`ASTRA_DB_TOKEN`) y el bundle se sube a `/content` o a `MyDrive/cpa/`. En local, se usan las variables de entorno `ASTRA_DB_TOKEN` y `ASTRA_DB_BUNDLE_PATH`, cargadas desde un archivo fuera del repo con permisos solo para el usuario.
- El código pide los secretos recién en la etapa de serving (`config.astra_secrets`): el resto del pipeline corre sin credenciales.
- El token es de aplicación con rol *Database Administrator*, porque crea la tabla. Se revoca al cerrar la materia o si se expone, y se reemplaza por uno nuevo.
- Las evidencias guardan rutas relativas al repo y nunca el token.

## Datos personales

`users.email` y los recursos etiquetados `pii:true` no pasan a Gold ni a AstraDB. En esta entrega `users` llega solo hasta Bronze; cuando tenga Silver, el email se excluye o se enmascara.
