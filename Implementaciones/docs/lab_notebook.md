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
