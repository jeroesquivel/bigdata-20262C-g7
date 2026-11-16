# Plan de correcciones (entrega 2)

## Devolución de la entrega 1

| Aspecto | Devolución |
|---|---|
| Nota | 10/10 |
| Correcciones pedidas | Ninguna |
| Penalización por uso de IA | 0,00 |
| Comentarios | El profesor la consideró una entrega de referencia y destacó el análisis del watermark: la simulación que muestra que un watermark sobre la fecha del evento descartaría entre el 46 % y el 92 % de los eventos, y la decisión de definirlo sobre `ingest_ts` (D5) |

Como no hubo correcciones pedidas, la tabla siguiente reúne las correcciones propias: inconsistencias que encontramos en el diseño al implementarlo y cambios de decisión. Prioridad: **alta** si afecta números o la ejecución, **media** si afecta la reproducibilidad o la trazabilidad, **baja** si es de redacción.

## Correcciones

| # | Qué se corrige | Prioridad | Responsable | Fecha | Estado y evidencia |
|---|---|---|---|---|---|
| C1 | El flujo MapReduce del mart (diseño, sección 8) separa el costo negativo con `< 0`, pero R5 marca los costos menores a −0,01. Eso deja 5 eventos (entre −0,01 y 0) en lugares distintos según dónde se mire. El mart toma el umbral de la configuración (`cost_negative_tolerance: -0.01`) | Alta | Pedro Salinas | 11/11/2026 | Hecho. [`src/cpa/gold.py`](../src/cpa/gold.py) usa `is_negative_cost`; `tests/test_gold.py` prueba que −0,005 se suma. El mart concilia contra Silver con 211 eventos negativos (`evidence/entrega2/gold_anomalias_*.md`) |
| C2 | El diseño (sección 8) dice que el mart sale en un archivo por reducer (8 archivos). Con 11.050 filas eso son 8 archivos chicos | Media | Pedro Salinas | 11/11/2026 | Hecho. Gold se escribe con `coalesce(1)`: un archivo por tabla (`evidence/entrega2/tamanos_rutas_*.md`) |
| C3 | Copia del lago a Drive (D22). Al implementar vimos que regenerar el lago desde Landing es más simple y rápido que copiar y restaurar cientos de archivos chicos | Media | Agustín Ronda | 11/11/2026 | Hecho. D22 reescrita en [`DECISIONS.md`](../DECISIONS.md). Las secciones 6.1, 6.8 y los riesgos R-3 y R-8 del diseño describen la versión anterior; la vigente está en [`arquitectura_v2.md`](arquitectura_v2.md) |
| C4 | Referencias a supuestos con el número equivocado en el diseño: la escala del CSAT citaba S5 (es S4) y el recálculo de eventos atrasados citaba S7 (es S6) | Baja | Jerónimo Esquivel | 13/11/2026 | Hecho en [`diseno_entrega1.md`](diseno_entrega1.md), secciones 3.3 y 7.3 |
| C5 | El README y el diseño citan el tag `v1.0-entrega1`, pero no existe en GitHub (verificado con `git ls-remote --tags` el 13/11/2026) | Media | Jerónimo Esquivel | 16/11/2026 | Hecho. Tags `v1.0-entrega1` (sobre `9be072f`) y `v2.0-entrega2` creados el 16/11/2026 |
| C6 | El README pedía Python 3.10 a 3.12. Colab hoy trae Python 3.13 y PySpark 4.0.4 preinstalado | Media | Agustín Ronda | 13/11/2026 | Hecho. Probado con Python 3.13, Java 17 y PySpark 3.5.3 (D2 se mantiene); el notebook instala la versión fijada en `requirements.txt`. Quickstart actualizado en el [README](../README.md) |
| C7 | La quarantine queda vacía con los datos reales, así que no se puede mostrar que funciona | Media | Pedro Salinas | 11/11/2026 | Hecho. Lote de prueba con errores inyectados (D23) en [`tests/fixtures/`](../tests/fixtures/README.md); muestras en `evidence/entrega2/quarantine_muestras_*.md` |
| C8 | El diccionario de datos cubría solo Landing y Bronze | Media | Jerónimo Esquivel | 13/11/2026 | Hecho. Se agregaron Silver y Gold en [`diccionario_datos.md`](diccionario_datos.md) |
| C9 | El diagrama v1 mostraba componentes que no se implementan en esta entrega | Media | Jerónimo Esquivel | 13/11/2026 | Hecho. Diagrama v2 con lo que corre en [`arquitectura_v2.md`](arquitectura_v2.md); el v1 queda en el diseño como referencia |

Las preguntas abiertas A1 a A7 siguen sin respuesta del profesor. En esta entrega se aplicó lo que proponíamos en cada una (por ejemplo, A2: los costos negativos van aparte en el mart; A4: el top-N de P2 se calcula en el cliente).
