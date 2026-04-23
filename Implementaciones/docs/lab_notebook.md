# Lab notebook

Registro cronológico de intentos, hallazgos y decisiones no canónicas que surgen durante la ejecución del plan de implementación. Complementa a `DECISIONS.md` (que fija decisiones estables) y a los commits de git (que registran cambios atómicos).

Convención: una sección por fecha. Dentro de cada fecha, un encabezado por tema.

---

## 2026-04-22

### Fase 0 y Fase 1 ejecutadas sin incidentes mayores

Se validó DuckPGQ en la máquina de trabajo y se fijó el stack (uv + Python 3.11 + DuckDB 1.4.4 + Anthropic Claude Haiku 4.5). Los detalles están en `DECISIONS.md`.

### Paso 3.2 — integración DuckDB ↔ SQLite de Spider

El script `notebooks/01_explore_spider.py` atacha la base SQLite de `concert_singer` vía la extensión `sqlite` de DuckDB y recupera correctamente las cuatro tablas (`concert`, `singer`, `singer_in_concert`, `stadium`) y las tres primeras preguntas de `dev.json`. No se requirió ningún ajuste de esquema.

### Paso 3.3 — XiYan-SQL como submódulo: hallazgo

El repositorio `https://github.com/XGenerationLab/XiYan-SQL.git` incorporado como submódulo en la Fase 2 **no contiene código ejecutable**. Es un meta-repositorio que documenta el framework y enlaza a los módulos reales, todos distribuidos como repos independientes:

- `XGenerationLab/XiYanSQL-QwenCoder` — modelos de generación (SQL backbone); disponible también vía ModelScope API.
- `alibaba/XiYan-SQL` — marco oficial de entrenamiento (agregado en octubre de 2025, mantiene sincronía con el repo anterior).
- `XGenerationLab/M-Schema` — representación semi-estructurada de esquemas.
- `XGenerationLab/xiyan_mcp_server` — servidor MCP.
- `XGenerationLab/XiYan-DBDescGen`, `XGenerationLab/XiYan-DateResolver`, `XGenerationLab/MoMQ` — utilidades y modelos auxiliares.

**Consecuencia.** Para el Paso 3.3 del plan (primera inferencia end-to-end), el submódulo actual no alcanza. Las alternativas evaluadas son:

1. Agregar como segundo submódulo `XiYanSQL-QwenCoder` y correr inferencia vía ModelScope API (gratuita con registro) o descargar pesos localmente.
2. Agregar el repo oficial `alibaba/XiYan-SQL` y usar su framework de entrenamiento / inferencia.
3. Posponer la integración de XiYan-SQL y validar la arquitectura del pipeline con el LLM inicial fijado en `DECISIONS.md` (Anthropic Claude Haiku 4.5). En esta variante el submódulo actual queda como referencia documental del framework hasta que se incorpore el generador real.

La decisión se toma a nivel humano (en la siguiente interacción) para no pivotear el stack sin consenso.

### Paso 3.3 — primera inferencia end-to-end (opción C elegida)

Se eligió la variante 3 (posponer XiYan-SQL y usar Claude Haiku 4.5). Se agregó `anthropic` y `python-dotenv` a las dependencias del proyecto y se creó `notebooks/02_first_inference.py` con el flujo mínimo: carga del esquema desde `tables.json`, formateo como bloque de tablas, llamada al modelo con un prompt del sistema breve, ejecución del SQL generado sobre la base SQLite vía DuckDB.

La primera pregunta de `concert_singer` (`How many singers do we have?`) produjo `SELECT COUNT(*) FROM singer;` — semánticamente equivalente al gold `SELECT count(*) FROM singer` — y la ejecución devolvió `(6,)`. Con esto queda validado que el pipeline mínimo corre de punta a punta para una pregunta.

La API key se inyectó como variable de entorno en la invocación y no quedó persistida en ningún archivo del repositorio. El archivo `.env.example` documenta la convención para que las credenciales vivan localmente en un `.env` ignorado por git.

### Paso 5 — primer experimento medible

Se implementó `evaluation/run_experiment_01.py`, que muestrea 20 preguntas de Spider de tres bases de complejidad creciente (`concert_singer`, `car_1`, `student_transcripts_tracking`) con seed fija, genera SQL con Claude Haiku 4.5, aplica el verificador estático y ejecuta cada consulta sobre DuckDB con la base SQLite original.

La primera corrida contaminó los resultados con un artefacto no previsto: Claude devolvía el SQL envuelto en bloques de código markdown (``` ```sql ... ``` ```) a pesar de que el prompt lo prohibía explícitamente. Tanto sqlglot como DuckDB rechazaban esos triple-backticks al inicio de la línea, lo que producía una tasa de detección aparentemente perfecta pero completamente espuria (5 de 5 "errores detectados" eran en realidad el mismo problema de formato en ambos sides de la comparación). Se agregó al script un post-procesamiento que extrae el cuerpo SQL del bloque markdown cuando aparece. Queda como nota para la discusión del capítulo 7 que las instrucciones explícitas de prompt no garantizan cumplimiento en modelos conversacionales.

La segunda corrida expuso un problema real del verificador MVP: los aliases definidos en el `SELECT` mediante `AS` generaban falsos positivos porque sqlglot los reporta como `exp.Column` al aparecer en `ORDER BY`. Se extendió `core/verifier/static.py` para recolectar los aliases definidos en la consulta y excluirlos del chequeo de columnas desconocidas, y se agregó `test_select_alias_not_flagged` como regresión.

La tercera corrida entregó números limpios: 20 consultas totales, 20 pasan la verificación estática, 19 ejecutan correctamente en DuckDB, 1 falla en ejecución. La única falla (`id 9` en `car_1`) es un error semántico de tipo (comparación BIGINT vs VARCHAR en un `IN`) que el verificador actual no puede detectar por diseño, ya que solo valida existencia de nombres. La tasa de detección estática para este sample resulta entonces en 0%.

Interpretación responsable: el sample no contiene alucinaciones de nombres porque Claude Haiku 4.5 no las produjo con esquemas de 4 a 11 tablas. Para que la métrica refleje la utilidad esperada del verificador hace falta o bien ampliar el volumen (100–200 preguntas), o bien evaluar con un modelo menos capaz, o bien diseñar preguntas con señuelo de esquema. El resultado positivo del experimento es otro: se validó el flujo completo de medición de punta a punta con métricas guardadas en JSON y reproducibilidad asegurada por seed.
