# Evidencia de la entrega 2

Todos los archivos salen de una misma corrida completa de [`notebooks/01_pipeline_entrega2.ipynb`](../../notebooks/01_pipeline_entrega2.ipynb), con el lago vacío al empezar. El sufijo de cada archivo es el `run_id` de la corrida (fecha y hora UTC); las evidencias nunca se pisan. El notebook se sube con sus salidas, así que cada número también se puede ver en su celda.

Entorno de la corrida: Python 3.13.12, Java 17 (OpenJDK 17.0.21), PySpark 3.5.3 en `local[*]`, spark-cassandra-connector 3.5.1, cassandra-driver 3.30.1 y AstraDB Serverless (AWS us-east-2). La consigna admite Colab "o un entorno equivalente validado"; los pasos para Colab están en el Quickstart del [README](../../README.md). Las rutas locales se reemplazan por `<repo>` y `~`.

| Archivo | Qué muestra | Resultado | Notebook |
|---|---|---|---|
| `bronze_batch_*.md` | Conteos Landing vs. Bronze de las 7 fuentes CSV, quarantine, archivos, bytes y ruta | Las 7 conservan sus filas (80, 800, 400, 1.000, 1.500, 92 y 240), 0 en quarantine, 1 archivo por fuente | Sección 2 |
| `bronze_stream_*.md` | Progreso de los micro-lotes (`recentProgress`), eventos fuera de orden, reinicio con el mismo checkpoint y contenido del checkpoint | 43.200 eventos en 12 micro-lotes, 0 descartados por watermark; cada lote trae eventos de los 60 días; el reinicio lee 0 filas | Sección 3 |
| `silver_calidad_*.md` | Conservación y reporte por regla | Bronze 43.200 = Silver 43.200 + quarantine 0 + duplicados 0; R5: 211, R6: 2.038, R13: 7.371 | Sección 4 |
| `explain_silver_join_*.txt` | Plan físico del join con las dimensiones | `BroadcastHashJoin` y `BroadcastExchange`, sin shuffle de los eventos | Sección 4 |
| `quarantine_muestras_*.md` | Muestras de quarantine del lote de prueba con errores inyectados (D23), no de los datos reales | Una muestra por regla bloqueante (R1, R3 ×2, R4 ×2, R6) y dos registros ilegibles | Sección 4.1 |
| `gold_anomalias_*.md` | Control del mart (grano, conciliación de costo), muestra con las seis features y anomalías con su sensibilidad al umbral | 11.050 filas, grano único, costo 147.433,9778 igual a Silver; 130 anomalías (1,18 %) con umbral 3,5 | Secciones 5 y 5.1 |
| `explain_gold_*.txt` | Plan físico del mart | `HashAggregate` parcial → `Exchange hashpartitioning(..., 8)` → `HashAggregate` final | Sección 5 |
| `serving_astradb_*.md` | Carga en AstraDB y resultados de P1 y P2 | 11.050 filas en AstraDB = Gold; P1: 64 filas (igual que Gold); P2: ranking de 6 servicios | Sección 6 |
| `cql_p1.png`, `cql_p2.png` | Capturas de P1 y P2 en la consola CQL del portal de AstraDB | | `cql/02_queries.cql` |
| `idempotencia_*.md` | Filas y archivos de 16 tablas (incluida AstraDB) antes y después de una segunda corrida completa | Sin cambios en las 16 | Sección 7 |
| `pytest_*.txt` | Salida de las pruebas | 11 pruebas pasan | Sección 8 |
| `run_log_*.jsonl` | Run log de las dos corridas: una fila por etapa con filas leídas, escritas, rechazadas y duplicadas, duración y estado | Todas las etapas en `ok` | Sección 9 |
| `tamanos_rutas_*.md` | Rutas del lago con cantidad de archivos y bytes | Un archivo por tabla en Gold, 60 en Silver de eventos (uno por día), 12 en Bronze de eventos (uno por micro-lote) | Sección 9 |
