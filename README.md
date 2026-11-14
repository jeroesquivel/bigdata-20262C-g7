# Cloud Provider Analytics: proyecto integrador de Big Data (ITBA 2C 2026)

Pipeline de datos para un proveedor de nube. Los eventos de uso se ingestan en streaming y los maestros de CRM y la facturación en batch. Con PySpark se procesan en un Data Lake en Parquet con cuatro zonas (Landing, Bronze, Silver y Gold), y los marts se publican en Cassandra/AstraDB.

Estado actual: **entrega 2**, pipeline end-to-end mínimo (v2.0, 16/11/2026). La entrega 1 (diseño, v1.0) está en `docs/diseno_entrega1.md`.

## Quickstart

El notebook [`notebooks/01_pipeline_entrega2.ipynb`](notebooks/01_pipeline_entrega2.ipynb) corre todo el pipeline: Landing → Bronze → Silver → Gold → AstraDB, una segunda corrida para mostrar la idempotencia y las pruebas. Tarda unos 4 minutos y deja la evidencia en `evidence/entrega2/`.

Versiones: Python 3.13, Java 17, PySpark 3.5.3, spark-cassandra-connector 3.5.1 (lo descarga Spark), cassandra-driver 3.30.1 (ver [`requirements.txt`](requirements.txt)).

### En Google Colab (entorno de referencia)

1. Abrir Colab, *Archivo → Abrir notebook → GitHub*, buscar `jeroesquivel/bigdata-20262C-g7` y elegir `notebooks/01_pipeline_entrega2.ipynb`. Entorno de ejecución por defecto (CPU).
2. Secretos de AstraDB (opcionales: sin ellos el pipeline corre igual y solo se saltea la carga a AstraDB):
   - Token: ícono de llave (*Secrets*) → agregar `ASTRA_DB_TOKEN` con el token de aplicación (`AstraCS:...`) y activar *Acceso del notebook*.
   - Bundle: subir `secure-connect-<base>.zip` a `/content` desde el panel *Archivos*.
3. *Entorno de ejecución → Ejecutar todas*. La primera celda instala Java 17, clona el repo en `/content/bigdata-20262C-g7` e instala `requirements.txt`. Colab trae otra versión de PySpark preinstalada; la celda instala la 3.5.3 antes de importarla.

El lago se genera en el disco de la sesión y se borra al cerrarla. En la sesión siguiente se vuelve a generar desde Landing, que viene con el repo (D22).

### En una máquina local

```bash
git clone https://github.com/jeroesquivel/bigdata-20262C-g7.git && cd bigdata-20262C-g7
python3.13 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt jupyter
export JAVA_HOME=<ruta a un JDK 17>          # macOS con Homebrew: brew install openjdk@17 y
                                             # JAVA_HOME=/opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home
# Opcional, para cargar AstraDB (nunca en el repo):
export ASTRA_DB_TOKEN='AstraCS:...'
export ASTRA_DB_BUNDLE_PATH=/ruta/a/secure-connect-<base>.zip
cd notebooks && jupyter nbconvert --to notebook --execute --inplace 01_pipeline_entrega2.ipynb
```

También se puede abrir el notebook en Jupyter y ejecutarlo celda por celda.

### Pruebas

```bash
python -m pytest -q tests        # 11 pruebas, unos 15 segundos; usan una sesión local de Spark
```

El notebook también las corre (sección 8) y guarda la salida en `evidence/entrega2/pytest_<run_id>.txt`.

### Limpieza

```bash
python scripts/reset_lake.py              # borra Bronze, Silver, Gold, quarantine y checkpoints (pide confirmación)
python scripts/reset_lake.py --si --incluir-meta                                  # también el run log
python scripts/reset_lake.py --si --config tests/fixtures/config_fixture.yaml     # el lago del lote de prueba
```

Landing no se borra nunca. Bronze de eventos y el checkpoint del stream se borran siempre juntos, para no duplicar ni perder eventos.

### Configuración

[`config/config.example.yaml`](config/config.example.yaml) tiene todos los parámetros: rutas del lago, Spark, streaming (`maxFilesPerTrigger`, watermark), umbrales de calidad y de anomalías, y AstraDB. Para cambiar algo, copiarlo a `config/config.yaml` (ignorado por git). Las variables `CPA_LAKE_ROOT` y `CPA_EVIDENCE_DIR` cambian la raíz del lago y la carpeta de evidencia.

### Problemas conocidos

| Síntoma | Causa y solución |
|---|---|
| Spark no arranca o falla con errores de la JVM | Usar Java 17: Spark 3.5 soporta oficialmente Java 8, 11 y 17, no 21. Revisar `JAVA_HOME` |
| En Colab se usa otra versión de PySpark | Se importó PySpark antes de la primera celda. *Reiniciar sesión* y ejecutar todo desde el principio |
| `NoHostAvailable` o timeout al conectar con AstraDB | La base del plan gratuito se hiberna después de 48 horas sin uso. Reanudarla desde el portal (*Resume*) y esperar a que esté *Active* |
| La sección 6 dice "Se saltea" | Faltan `ASTRA_DB_TOKEN` o el bundle (ver el paso 2 del Quickstart) |
| En una Mac, Spark usa la IP de la red en lugar de `127.0.0.1` | Ya está resuelto en `spark_session.py` (`SPARK_LOCAL_IP` y `spark.driver.host`) |

## Artefactos de la entrega 2

| Ítem del checklist (consigna 9.2) | Dónde está |
|---|---|
| Correcciones de la primera entrega | [docs/plan_correcciones.md](docs/plan_correcciones.md) |
| Arquitectura actualizada | [docs/arquitectura_v2.md](docs/arquitectura_v2.md) |
| Tres maestros batch en Bronze | Las 7 fuentes CSV: [src/cpa/bronze_batch.py](src/cpa/bronze_batch.py); notebook, sección 2 |
| Streaming con esquema, watermark, dedupe, late data y checkpoint | [src/cpa/bronze_stream.py](src/cpa/bronze_stream.py); notebook, sección 3 (progreso por micro-lote, eventos fuera de orden y reinicio con el mismo checkpoint) |
| Silver para eventos y al menos un maestro | [src/cpa/silver.py](src/cpa/silver.py): `usage_events`, `dim_org` y `dim_resource`; notebook, sección 4 |
| Tres features calculadas | `daily_cost_usd`, `requests`, `cpu_hours`, `storage_gb_hours`, `genai_tokens` y `carbon_kg` en [src/cpa/gold.py](src/cpa/gold.py) |
| Tres reglas de calidad y quarantine | R1 a R7 y R13 en [src/cpa/quality.py](src/cpa/quality.py); muestras de quarantine con el lote de prueba de [tests/fixtures/](tests/fixtures/README.md) (D23) |
| Mart `org_daily_usage_by_service` en Gold | [src/cpa/gold.py](src/cpa/gold.py); notebook, sección 5 |
| Componente analítico | Anomalías de costo con z-score robusto (MAD) en [src/cpa/anomalies.py](src/cpa/anomalies.py); notebook, sección 5.1 (D11, D24) |
| Keyspace y tabla Cassandra/AstraDB poblada | [cql/01_schema.cql](cql/01_schema.cql) y [src/cpa/serving.py](src/cpa/serving.py); notebook, sección 6 (D25) |
| Dos consultas CQL con resultados | [cql/02_queries.cql](cql/02_queries.cql); resultados en `evidence/entrega2/serving_astradb_*.md` y capturas `cql_p1.png` y `cql_p2.png` |
| Idempotencia demostrada | Notebook, sección 7; `evidence/entrega2/idempotencia_*.md` |
| README Quickstart, logs y evidencias | Este archivo; índice de evidencias en [evidence/entrega2/README.md](evidence/entrega2/README.md) |
| Gobierno preliminar | [docs/gobierno.md](docs/gobierno.md) |
| Backlog final priorizado | [docs/backlog_final.md](docs/backlog_final.md) |

## Estructura del repositorio

```text
README.md               este archivo
DECISIONS.md            decisiones tomadas, alternativas descartadas y preguntas abiertas
requirements.txt        dependencias con versión fija
config/                 configuración de ejemplo, sin secretos
src/cpa/                código del pipeline (una etapa por módulo)
notebooks/              00_exploracion.ipynb (entrega 1) y 01_pipeline_entrega2.ipynb
cql/                    esquema y consultas de AstraDB
tests/                  pruebas con pytest y lote de prueba con errores inyectados (fixtures/)
scripts/                reset_lake.py
docs/                   diseño (entrega 1), arquitectura v2, diccionario, gobierno, correcciones y backlog
datalake/landing/       datos de muestra de la cátedra (no se modifican)
evidence/               salidas de ejecución de cada entrega
```

Las carpetas `bronze/`, `silver/`, `gold/`, `quarantine/`, `_meta/` y `_checkpoints/` dentro de `datalake/`, y el lago del lote de prueba (`datalake_fixture/`), las genera el pipeline y no se suben al repo.

## Cómo reproducir la exploración (entrega 1)

`notebooks/00_exploracion.ipynb` perfila las 8 fuentes de Landing y genera `evidence/entrega1/perfil_landing.json`. Se corre desde `notebooks/` después de instalar `requirements.txt` (en Colab, primero `!pip install -r requirements.txt` desde la raíz del repo clonado). La ruta de Landing se puede cambiar con la variable de entorno `LANDING_DIR`.

## Convenciones

- Landing es de solo lectura: ningún proceso escribe, mueve ni modifica archivos de `datalake/landing/`.
- Nombres en `snake_case` y en inglés para tablas, columnas y rutas. Las rutas siguen el patrón `<zona>/<entidad>/<particion>=<valor>/` y los marts de Gold se llaman `<hecho>_by_<grano>`.
- Columnas técnicas en Bronze: `ingest_ts`, `source_file` e `ingest_date`.
- Los flags de calidad se calculan en Silver y son booleanos (`is_negative_cost`, `unit_imputed`, `is_cost_anomaly`, etc.). Las reglas bloqueantes que no se cumplen se listan en `dq_errors`, y esos registros van a quarantine.
- Todos los timestamps están en UTC (`spark.sql.session.timeZone=UTC`). Las ventanas de "últimos N días" se calculan contra `as_of_date`, que es configurable.
- Los secretos no se suben al repo. El token de AstraDB y el *secure connect bundle* van en variables de entorno o en Colab Secrets (ver [docs/gobierno.md](docs/gobierno.md#secretos)).
- Cada etapa registra filas leídas, escritas, rechazadas y duplicadas, y su duración, en `_meta/run_log/`. Las evidencias llevan el `run_id` en el nombre y no se pisan.
- Cada entrega se marca con un tag (`v1.0-entrega1`, `v2.0-entrega2`, `v3.0-final`). Lo que se cambie después del corte se registra como corrección a partir del feedback.
- Las decisiones técnicas importantes se anotan en `DECISIONS.md` antes de implementarlas.

## Limitaciones conocidas

- Con una muestra de 12,6 MB no se puede mostrar rendimiento a escala. La justificación de Big Data se apoya en una proyección explícita (sección 2 del documento de diseño); la evidencia de rendimiento son los planes físicos (`explain`), las particiones y los tamaños de archivo.
- Spark corre en modo local, en una sola máquina.
- Silver y Gold se recalculan completos en cada corrida (D7). A escala habría que recalcular solo las fechas afectadas.
- Solo `usage_events` y las dimensiones de organización y recurso llegan a Silver, y solo un mart a AstraDB. El resto está en el backlog de la entrega final.
