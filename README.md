# Cloud Provider Analytics: proyecto integrador de Big Data (ITBA 2C 2026)

Pipeline de datos para un proveedor de nube. Los eventos de uso se ingestan en streaming y los maestros de CRM y la facturación en batch. Con PySpark se procesan en un Data Lake en Parquet con cuatro zonas (Landing, Bronze, Silver y Gold), y los marts para FinOps, Soporte y Producto se publican en Cassandra/AstraDB.

Estado actual: entrega 1, diseño y fundación de datos (v1.0, entregada el 05/10/2026). Todavía no hay código del pipeline; eso es parte de la entrega 2.

## Artefactos de la entrega 1

| Artefacto (sección 5.3 de la consigna) | Dónde está |
|---|---|
| Documento de diseño | [docs/diseno_entrega1.md](docs/diseno_entrega1.md) |
| Diagrama de arquitectura v1 | [Documento de diseño, sección 4.1](docs/diseno_entrega1.md#41-diagrama-v10-04102026) |
| Matriz requisito-componente | [Documento de diseño, sección 9](docs/diseno_entrega1.md#9-matriz-requisito-componente) |
| Plan inicial (supuestos, riesgos, esfuerzo) | [Documento de diseño, sección 10](docs/diseno_entrega1.md#10-plan-inicial) |
| Registro de decisiones | [DECISIONS.md](DECISIONS.md) |
| Diccionario de datos (borrador) | [docs/diccionario_datos.md](docs/diccionario_datos.md) |
| Evidencia de exploración | [notebooks/00_exploracion.ipynb](notebooks/00_exploracion.ipynb) y [evidence/entrega1/](evidence/entrega1/) |

## Arquitectura en pocas palabras

`Landing (CSV/JSONL)` → ingesta batch y Structured Streaming → `Bronze` → `Silver` → `Gold` (Parquet) → `AstraDB` → consultas CQL.

El patrón es híbrido. El streaming solo se usa para ingestar los eventos en Bronze; Silver y Gold se recalculan en batch, así que cada transformación se escribe una sola vez. El detalle y las alternativas que descartamos están en el [documento de diseño](docs/diseno_entrega1.md).

## Estructura del repositorio

```text
README.md               este archivo
DECISIONS.md            decisiones tomadas, alternativas descartadas y preguntas abiertas
requirements.txt        dependencias con versión fija
config/                 configuración de ejemplo, sin secretos
datalake/landing/       datos de muestra de la cátedra (no se modifican)
docs/                   documento de diseño y diccionario de datos
notebooks/              exploración (00_exploracion.ipynb)
evidence/entrega1/      salidas de ejecución que respaldan el documento
```

En la entrega 2 se agregan `src/` (jobs de ingesta, procesamiento y serving), `tests/` y los scripts CQL. Las carpetas `bronze/`, `silver/`, `gold/`, `quarantine/`, `_meta/` y `_checkpoints/` las genera el pipeline y no se suben al repo.

## Cómo reproducir la exploración

En Google Colab, que es el entorno de referencia:

```python
!git clone https://github.com/jeroesquivel/bigdata-20262C-g7.git cpa
%cd cpa/notebooks
# Abrir 00_exploracion.ipynb y ejecutar todo. Si falta pyspark==3.5.3, el notebook lo instala.
```

En una máquina local:

- Hace falta Python 3.10 a 3.12 (lo probamos con 3.10 y 3.12) y Java 17. Spark 3.5 soporta Java 8, 11 y 17, así que no conviene usar una versión más nueva.
- `pip install -r requirements.txt jupyter` y después ejecutar `notebooks/00_exploracion.ipynb` desde la carpeta `notebooks/`.
- En Windows hay que definir `PYSPARK_PYTHON` con el intérprete del entorno virtual. El notebook le pasa a Spark la lista de archivos, así que no hace falta `winutils.exe` para leer.

Al terminar, el notebook muestra el perfil de las 8 fuentes (43.200 eventos) y genera `evidence/entrega1/perfil_landing.json`. La ruta de Landing se puede cambiar con la variable de entorno `LANDING_DIR`.

## Convenciones

- Landing es de solo lectura: ningún proceso escribe, mueve ni modifica archivos de `datalake/landing/`.
- Nombres en `snake_case` y en inglés para tablas, columnas y rutas. Las rutas siguen el patrón `<zona>/<entidad>/<particion>=<valor>/` y los marts de Gold se llaman `<hecho>_by_<grano>`.
- Columnas técnicas en Bronze: `ingest_ts`, `source_file` e `ingest_date`.
- Los flags de calidad se calculan en Silver y son booleanos (`is_negative_cost`, `unit_imputed`, `is_cost_anomaly`, etc.). Las reglas que no se cumplen se listan en `dq_errors`, también en quarantine.
- Todos los timestamps están en UTC (`spark.sql.session.timeZone=UTC`). Las ventanas de "últimos N días" se calculan contra `as_of_date`, que es configurable.
- Los secretos no se suben al repo. El token de AstraDB y el *secure connect bundle* van en variables de entorno o en Colab Secrets (ver `config/config.example.yaml`).
- Cada entrega se marca con un tag (`v1.0-entrega1`, `v2.0-entrega2`, `v3.0-final`). Lo que se cambie después del corte se registra como corrección a partir del feedback.
- Las decisiones técnicas importantes se anotan en `DECISIONS.md` antes de implementarlas.

## Limitaciones conocidas

- Con una muestra de 12,6 MB no se puede mostrar rendimiento a escala. La justificación de Big Data se apoya en una proyección explícita (sección 2 del documento de diseño).
- Spark corre en modo local, en una sola máquina de Colab.
