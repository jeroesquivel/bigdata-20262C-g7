# Evidencia — Entrega 1

| Archivo | Contenido | Cómo se generó |
|---|---|---|
| `perfil_landing.json` | Perfil de las 8 fuentes: filas, nulos, valores distintos, unicidad de claves, integridad referencial, chequeos de dominio, chequeos de consistencia entre columnas y tablas (`chequeos_consistencia` para maestros y facturación, `chequeos_consistencia_eventos` para eventos ↔ recursos y series de costo diario), evolución de esquema, tipo JSON de `value`, distribución de CSAT, rangos de fechas, fechas de modificación de los archivos de eventos (`mtimes_distintos`) y simulación del watermark por event-time | Ejecución completa de [`notebooks/00_exploracion.ipynb`](../../notebooks/00_exploracion.ipynb) |
| `../../notebooks/00_exploracion.ipynb` | El notebook se versiona **con sus salidas**: cada cifra del documento de diseño se puede ver en una celda, incluidas las que no están en el JSON (tamaños por archivo, severidad de tickets, metric × unit, tipo JSON de `carbon_kg`, rango temporal por archivo) | Ídem |

**Entorno de la ejecución registrada:** PySpark 3.5.3, `local[*]`, Java 17 (Temurin 17.0.20.1+1), Python 3.10.12, `spark.sql.session.timeZone=UTC`. La fecha UTC de generación está en el campo `generado_utc` del JSON.

**Para regenerar:** ver "Cómo reproducir la exploración" en el [README](../../README.md). Las cifras dependen solo de los archivos de Landing, que son inmutables, así que una nueva ejecución debe producir los mismos valores (salvo `generado_utc` y `mtimes_distintos`, que depende de las fechas de modificación de la copia local de los archivos).
