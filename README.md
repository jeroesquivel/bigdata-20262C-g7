# Cloud Provider Analytics — Proyecto Integrador Big Data (ITBA 2C 2026)

Pipeline de datos para un proveedor de nube. Ingesta los eventos de uso en **streaming** y los maestros de CRM y la facturación en **batch**. Los conforma en un Data Lake **Parquet** de cuatro zonas (Landing, Bronze, Silver y Gold) con **PySpark** y publica marts analíticos para FinOps, Soporte y Producto en **Cassandra/AstraDB**.

**Estado:** entrega 1, diseño y fundación de datos (v1.0, entrega el 05/10/2026). Todavía no hay código de pipeline: eso corresponde a la entrega 2.

## Artefactos de la entrega 1

| Artefacto (consigna §5.3) | Ubicación |
|---|---|
| Documento de diseño | [docs/diseno_entrega1.md](docs/diseno_entrega1.md) |
| Diagrama de arquitectura v1 | [docs/diseno_entrega1.md §4.1](docs/diseno_entrega1.md#41-diagrama--v10--04102026) |
| Matriz requisito-componente | [docs/diseno_entrega1.md §9](docs/diseno_entrega1.md#9-matriz-requisito--componente) |
| Plan inicial (supuestos, riesgos, esfuerzo) | [docs/diseno_entrega1.md §10](docs/diseno_entrega1.md#10-plan-inicial) |
| Registro de decisiones | [DECISIONS.md](DECISIONS.md) |
| Diccionario de datos (borrador) | [docs/diccionario_datos.md](docs/diccionario_datos.md) |
| Evidencia de exploración | [notebooks/00_exploracion.ipynb](notebooks/00_exploracion.ipynb) · [evidence/entrega1/](evidence/entrega1/) |

## Arquitectura en una línea

`Landing (CSV/JSONL)` → ingesta batch y Structured Streaming → `Bronze` → `Silver` → `Gold` (Parquet en Google Drive) → `AstraDB` → consultas CQL.

El patrón es **híbrido: ingesta streaming + conformado batch**, una variante de Lambda sin capa de velocidad. El streaming solo ingesta eventos a Bronze, y Silver y Gold se recalculan en batch, con un solo motor y una sola implementación de cada transformación. Los detalles y las alternativas descartadas están en el [documento de diseño](docs/diseno_entrega1.md).

## Estructura del repositorio

```text
README.md               este archivo
DECISIONS.md            decisiones, alternativas descartadas y decisiones abiertas
requirements.txt        dependencias con versión fija
config/                 configuración externalizada (solo ejemplos, sin secretos)
datalake/landing/       datos de muestra provistos — INMUTABLES, no modificar
docs/                   documento de diseño y diccionario de datos
notebooks/              exploración (00_exploracion.ipynb)
evidence/entrega1/      salidas de ejecución que respaldan el documento
```

El material de la cátedra (consigna, planificación y slides de clase) que pueda haber en `docs/*.pdf` no se versiona.

En la entrega 2 se agregan `src/` (jobs de ingesta, procesamiento y serving), `tests/` y los scripts CQL. Las zonas `bronze/`, `silver/`, `gold/`, `quarantine/`, `_meta/` y `_checkpoints/` se generan al ejecutar el pipeline y no se versionan.

## Cómo reproducir la exploración

**Google Colab (entorno de referencia)**

```python
!git clone https://github.com/jeroesquivel/bigdata-20262C-g7.git cpa
%cd cpa/notebooks
# Abrir 00_exploracion.ipynb y ejecutar todo: instala pyspark==3.5.3 si falta.
```

**Local**

- Requisitos: Python 3.10–3.12 (probado con 3.10 y 3.12) y Java 17. Spark 3.5 soporta oficialmente Java 8, 11 y 17; no usar versiones más nuevas.
- `pip install -r requirements.txt jupyter` y luego ejecutar `notebooks/00_exploracion.ipynb` desde la carpeta `notebooks/`.
- **Windows:** definir `PYSPARK_PYTHON` con el intérprete del entorno virtual. El notebook pasa a Spark la lista explícita de archivos, así que no necesita `winutils.exe` para leer.

**Salida esperada:** el perfil impreso en el notebook y el archivo `evidence/entrega1/perfil_landing.json` (43.200 eventos, 8 fuentes). La ruta de Landing se puede cambiar con la variable de entorno `LANDING_DIR`.

## Convenciones

- **Landing es de solo lectura.** Ningún proceso escribe, mueve ni modifica archivos de `datalake/landing/`.
- **Nombres:** `snake_case` en inglés para tablas, columnas y rutas. Las rutas siguen el patrón `<zona>/<entidad>/<particion>=<valor>/`. Los marts de Gold se nombran `<hecho>_by_<grano>`.
- **Columnas técnicas** en Bronze: `ingest_ts`, `source_file` e `ingest_date`.
- **Calidad:** los flags se calculan en Silver y son booleanos (`is_negative_cost`, `unit_imputed`, `is_cost_anomaly`, …). La lista de reglas incumplidas va en `dq_errors`, también en los registros de quarantine.
- **Tiempo:** todos los timestamps están en UTC (`spark.sql.session.timeZone=UTC`). Las ventanas "últimos N días" se calculan contra `as_of_date`, que se configura.
- **Secretos:** nunca se versionan. El token de AstraDB y el *secure connect bundle* van en variables de entorno o en Colab Secrets (ver `config/config.example.yaml`).
- **Versionado:** se crea un tag por entrega (`v1.0-entrega1`, `v2.0-entrega2`, `v3.0-final`). Los cambios posteriores al corte se registran como correcciones derivadas del feedback.
- **Decisiones:** toda decisión técnica relevante se registra en `DECISIONS.md` antes de implementarla.

## Limitaciones conocidas

- La muestra (12,6 MB) no permite demostrar performance a escala. La justificación de Big Data se basa en una proyección explícita (documento de diseño §2).
- Spark corre en modo local (un solo nodo) en Colab.
