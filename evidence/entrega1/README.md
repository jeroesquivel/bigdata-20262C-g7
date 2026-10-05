# Evidencia de la entrega 1

| Archivo | Qué contiene | Cómo se generó |
|---|---|---|
| `perfil_landing.json` | El perfil de las 8 fuentes: filas, nulos, valores distintos, unicidad de claves, integridad referencial, chequeos de dominio y de consistencia entre columnas y tablas (`chequeos_consistencia` para maestros y facturación, `chequeos_consistencia_eventos` para eventos contra recursos y series de costo diario), cambio de esquema, tipos de `value`, distribución del CSAT, rangos de fechas, fechas de modificación de los archivos de eventos (`mtimes_distintos`) y la simulación del watermark sobre la fecha del evento | Ejecutando completo [`notebooks/00_exploracion.ipynb`](../../notebooks/00_exploracion.ipynb) |
| `../../notebooks/00_exploracion.ipynb` | El notebook se sube con sus salidas, así que cada número del documento de diseño se puede ver en alguna celda. Incluye datos que no están en el JSON, como el tamaño de cada archivo, la severidad de los tickets, la unidad de cada métrica, el tipo de `carbon_kg` y el rango de fechas de cada archivo | Igual que el anterior |

La ejecución que quedó registrada usó PySpark 3.5.3 en modo `local[*]`, Java 17 (Temurin 17.0.20.1+1), Python 3.10.12 y `spark.sql.session.timeZone=UTC`. La fecha de generación está en el campo `generado_utc` del JSON.

Para regenerarla, seguir "Cómo reproducir la exploración" en el [README](../../README.md). Los números dependen solo de los archivos de Landing, que no cambian, así que otra ejecución tiene que dar lo mismo. Las excepciones son `generado_utc` y `mtimes_distintos`, que depende de las fechas de modificación de la copia local de los archivos.
