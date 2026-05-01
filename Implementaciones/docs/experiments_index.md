# Índice de experimentos

Documento de referencia con la configuración, resultados y trazabilidad de cada
experimento ejecutado sobre el pipeline de la tesis. Cada entrada apunta al
script generador, al JSON con los resultados crudos y al commit de git que
introdujo el código. Para el contexto narrativo, las decisiones intermedias y
la discusión metodológica ver `lab_notebook.md`.

Las cifras de tokens y costo se calcularon con la tarifa pública de Anthropic
Haiku 4.5 al momento de la corrida (USD 0,80/Mtok input, USD 4/Mtok output).

## Tabla resumen

| Exp. | Pregunta de investigación | Sample | DBs | Engine | Métrica clave | Costo USD |
|:---:|---|---:|---:|:---:|---|---:|
| 01 | ¿el flujo end-to-end corre sobre Spider? | 20 | 3 | DuckDB | 19/20 ejecutan; tasa de detección 0/1 (denominador chico) | ~0,01 |
| 02 | ¿escala a más preguntas y bases? ¿qué tipos de error aparecen? | 100 | 6 | DuckDB | 90/100 ejecutan; 0/10 detectados; 5 de 10 son data-quality | 0,049 |
| 03 | ¿cuánto del error rate viene del motor estricto vs del LLM? | 100 (predicciones reusadas de 02) | 6 | sqlite3 | 98/100 ejecutan; DuckDB resulta ser superset estricto (+8 fallos) | 0,00 |
| 04 | ¿el verificador detecta alucinaciones cuando ocurren? | 15 adv. + 5 control | 3 | sqlite3 | 1/1 alucinación real atrapada; 0 falsos positivos en controles | 0,008 |

## Configuración detallada

| Exp. | Script | Output JSON | Modelo | Seed | Motor | Notas |
|:---:|---|---|---|:---:|:---:|---|
| 01 | `evaluation/run_experiment_01.py` | `runs/experiment_01_*.json` | claude-haiku-4-5-20251001 | 42 | DuckDB 1.4.4 + ext. `duckpgq` | Sample muestreado; primera corrida tuvo bug de verificador (FPs por aliases) y bug del LLM (markdown fences) — ambos corregidos antes del número final |
| 02 | `evaluation/run_experiment_02.py` | `runs/experiment_02_*.json` | claude-haiku-4-5-20251001 | 42 | DuckDB 1.4.4 + ext. `duckpgq` | `temperature=0` para reproducibilidad; helpers compartidos vía `evaluation/_helpers.py` |
| 03 | `evaluation/run_experiment_03.py` | `runs/experiment_03_*.json` | (no llama al LLM) | 42 (heredado) | sqlite3 stdlib | Reutiliza exactamente las predicciones de la corrida 02 más reciente; aísla el efecto del motor |
| 04 | `evaluation/run_experiment_04.py` | `runs/experiment_04_*.json` | claude-haiku-4-5-20251001 | n/a (corpus fijo) | sqlite3 stdlib | Corpus fijo en `corpus/adversarial/spider_decoys.json`; 15 adversariales + 5 controles |

## Tiempo y consumo

| Exp. | Llamadas LLM | Tokens in | Tokens out | Tiempo | Costo USD |
|:---:|---:|---:|---:|---:|---:|
| 01 | 20 | (no instrumentado) | (no instrumentado) | ~30 s | ~0,01 |
| 02 | 100 | 36 697 | 4 996 | 111,2 s | 0,049 |
| 03 | 0 | 0 | 0 | ~3 s | 0,00 |
| 04 | 20 | 5 944 | 893 | 25,2 s | 0,008 |
| Total | 140 | 42 641 | 5 889 | ~169 s | ~0,07 |

## Detalle por experimento

### Experimento 01 — primer prototipo end-to-end

**Pregunta**. ¿El pipeline `NL → SQL (LLM) → verificador → ejecución` corre sin intervención manual sobre una muestra real de Spider?

**Sample**. 20 preguntas estratificadas en tres bases de complejidad creciente:

| db_id | tablas | columnas | preguntas en dev | sampleadas |
|---|---:|---:|---:|---:|
| concert_singer | 4 | 22 | 45 | 7 |
| car_1 | 6 | 24 | 92 | 7 |
| student_transcripts_tracking | 11 | 57 | 78 | 6 |

**Resultado final**. 19 ejecutan correctamente; 1 falla. El error es semántico (incompatibilidad de tipos `BIGINT` vs `VARCHAR` en cláusula `IN`) — fuera del alcance del verificador MVP por diseño. Cero falsos positivos del verificador en este sample.

**Hallazgos colaterales**.
- *Markdown fence artifact*: 5 de las 20 respuestas iniciales del LLM venían envueltas en bloques `` ```sql ... ``` ``, contradiciendo el system prompt. Se agregó post-procesamiento en `_helpers.extract_sql`.
- *Bug del verificador*: aliases declarados con `AS` en el `SELECT` y reutilizados en `ORDER BY` se reportaban como `unknown_column`. Cubierto con test de regresión `test_select_alias_not_flagged` y corregido.
- *No-determinismo del LLM*: dos corridas idénticas devolvían números levemente distintos. Se fijó `temperature=0` en el helper compartido para reducir varianza.

**Commits asociados**: [fcbb249] feat: primer experimento medible sobre 20 preguntas de Spider · [20e8dbc] fix: falsos positivos del verificador estático por aliases AS · [5fa6fde] feat: segundo experimento sobre 100 preguntas con helpers compartidos (refactor + temperature=0).

---

### Experimento 02 — escala a 100 preguntas en seis bases

**Pregunta**. ¿La métrica del experimento 01 sostiene a mayor volumen y con mayor variedad de esquemas? ¿Aparecen tipos de error que el sample chico no había expuesto?

**Sample**. 100 preguntas distribuidas estratificadamente en seis bases:

| db_id | tablas | columnas | sampleadas |
|---|---:|---:|---:|
| concert_singer | 4 | 22 | 17 |
| world_1 | 4 | 27 | 17 |
| car_1 | 6 | 24 | 17 |
| wta_1 | 3 | 44 | 17 |
| dog_kennels | 8 | 50 | 16 |
| student_transcripts_tracking | 11 | 57 | 16 |

**Resultado**. 90 ejecutan, 10 fallan, 0 detectados por el verificador, 0 falsos positivos.

**Taxonomía manual de los 10 fallos**.

| Tipo | Conteo | Detectable por verificador MVP |
|---|:---:|:---:|
| Type mismatch (BINDER en `IN`) | 1 | No (requiere tipos en IR) |
| `GROUP BY` violado | 2 | No (requiere análisis de scope agregado) |
| `Conversion Error` por `birth_date=""` | 5 | No (es data quality, no error del LLM) |
| `Mismatch Type Error` por integer con string vacío | 2 | No (mismo origen que el anterior) |

Los siete fallos de las dos últimas filas corresponden a problemas de calidad de datos en las SQLite originales de Spider: SQLite es laxo con los tipos y tolera strings vacíos donde el esquema declara `INTEGER` o `DATE`; DuckDB es estricto y los rechaza. La base `wta_1` concentra 7 de los 10 fallos por esta razón, no por dificultad intrínseca de las preguntas.

**Desglose por base** (sobre el sample de 100):

| db_id | n | ejecutan | flagead. | fallos |
|---|---:|---:|---:|---:|
| concert_singer | 17 | 16 | 17 | 1 |
| world_1 | 17 | 16 | 17 | 1 |
| car_1 | 17 | 16 | 17 | 1 |
| wta_1 | 17 | 10 | 17 | 7 |
| dog_kennels | 16 | 16 | 16 | 0 |
| student_transcripts_tracking | 16 | 16 | 16 | 0 |

**Commit asociado**: [5fa6fde] feat: segundo experimento sobre 100 preguntas con helpers compartidos.

---

### Experimento 03 — comparación de motores de ejecución

**Pregunta**. ¿Qué fracción de los 10 fallos del experimento 02 es atribuible al motor estricto y qué fracción al LLM?

**Diseño**. Se cargan las predicciones del experimento 02 más reciente y se re-ejecutan sobre `sqlite3` (módulo de la stdlib, motor laxo, mismo que la literatura sobre Spider). No se vuelve a llamar al LLM; las predicciones son idénticas. La única variable es el motor.

**Resultado**.

| Motor | Ejecutan | Tasa | Lectura |
|---|---:|---:|---|
| DuckDB 1.4.4 (estricto) | 90 / 100 | 90,0 % | upper bound; útil internamente para SQL/PGQ |
| **sqlite3 stdlib (laxo)** | **98 / 100** | **98,0 %** | comparable con XiYan-SQL, CHESS, BIRD-leaderboard |

**Cruce de motores**.

|  | DuckDB OK | DuckDB falla |
|---|---:|---:|
| **sqlite3 OK** | 90 | 8 |
| **sqlite3 falla** | 0 | 2 |

La asimetría es clara: 8 fallos del experimento 02 desaparecen al cambiar el motor, 0 fallos aparecen al hacer el cambio. DuckDB se comporta como un superset estricto de chequeos respecto de sqlite3 sobre las SQLite originales de Spider. Solo 2 fallos sobreviven al cambio de motor: ambos son errores semánticos genuinos del LLM (`GROUP BY` violado).

**Implicación para el reporte**. La línea base comparable con la literatura es **98 % de execution accuracy de Claude Haiku 4.5** en este sample de Spider dev. El número en DuckDB es métrica interna útil para el pipeline PGQ donde la data es generada por nosotros y no presenta el mismo problema de tipado.

**Commit asociado**: [737e672] feat: segundo motor de ejecución para evaluar Spider con sqlite3.

---

### Experimento 04 — evaluación adversarial del verificador

**Pregunta**. Si la tasa de alucinación de Haiku 4.5 sobre Spider aleatorio es ≈ 0 (de modo que el verificador no tiene oportunidad de actuar), ¿qué ocurre cuando las preguntas se construyen específicamente para inducir alucinaciones? ¿Atrapa el verificador las alucinaciones que sí ocurren?

**Diseño**. Corpus fijo en `corpus/adversarial/spider_decoys.json` con 20 preguntas escritas a mano sobre tres bases compartidas con experimentos previos:

- 15 adversariales en cuatro categorías (`missing-column` ×10, `missing-table` ×3, `decoy-rename` ×1, `implicit-attribute` ×1).
- 5 controles directamente respondibles con el esquema, para medir falsos positivos del verificador.

**Resultado plano**.

| Subconjunto | n | LLM alucinó | verificador flageó | sqlite ejecuta |
|---|---:|---:|---:|---:|
| adversariales | 15 | 1 | 4 | 11 |
| controles | 5 | 0 | 0 | 5 |

La diferencia entre "LLM alucinó" (1) y "verificador flageó" (4) requiere análisis caso por caso, hecho en la entrada correspondiente del lab notebook. Los 4 cases flageados se descomponen así:

| Comportamiento real del LLM | Conteo | Verificador | sqlite | Lectura |
|---|:---:|:---:|:---:|---|
| Alucinación de columna (`Color` en `cars_data`) | 1 | flagea `unknown_column: Color` | falla `no such column` | TP del verificador |
| Refusal en prosa ("I cannot answer...") | 3 | flagea `parse_error` | falla `near "I": syntax error` | TP estructural (la salida no es SQL válido), aunque no es alucinación |
| Reformulación con columnas reales | 6 | OK | ejecuta | comportamiento esperable, no falla |
| Pivoteo conservador (NULL placeholder, columna semántica cercana) | 4 | OK | ejecuta | no detectable estáticamente; tampoco es error |
| Bug del corpus (`winner_ht`/`loser_ht` en `matches`) | 1 | OK | ejecuta | mi error de diseño, no del modelo |

**Métricas reformuladas**.

- *Recall sobre alucinaciones reales*: 1/1 = 100 % (denominador muy chico).
- *Recall sobre salidas no parseables*: 3/3 = 100 %.
- *Especificidad sobre controles*: 5/5 = 100 %.
- *Frecuencia de alucinación de Haiku 4.5 con prompt explícito sobre 15 adversariales bien diseñadas*: 1/14 = 7 % (excluyo el caso del bug del corpus).

**Lectura para la tesis**. La hipótesis implícita del verificador (LLMs alucinan con frecuencia y conviene atraparlos antes de ejecutar) **no se sostiene en este régimen**. Haiku 4.5 con esquema explícito prefiere reformular, devolver NULL o rehusarse en prosa antes que inventar nombres. La contribución del verificador se entiende mejor como una **garantía estructural de soundness** (cuando el SQL pasa la verificación, está garantizado que solo referencia nombres del esquema), más cercana a un type-checker que a un detector estadístico. La métrica natural deja de ser la tasa de detección y pasa a ser una propiedad cualitativa.

**Commit asociado**: [486a5ba] feat: corpus adversarial mínimo y experimento de evaluación del verificador.

---

## Glosario de métricas

- **Ejecuta**: la consulta SQL es aceptada y produce resultado por el motor configurado, sin importar si el resultado es semánticamente correcto. Sinónimo de *execution accuracy* en la literatura, condicionado al motor.
- **Verificador flagea**: la verificación estática devuelve al menos un error.
- **Tasa de detección estática** (=*recall* sobre errores de ejecución): proporción de queries que fallan en ejecución y que el verificador ya había flageado.
- **Falso positivo (FP) del verificador**: el verificador flagea una consulta que ejecutaría sin error.
- **Especificidad**: 1 − tasa de FP. Sobre controles, fracción que el verificador no flagea.
- **Alucinación de esquema**: la consulta referencia un nombre de tabla o columna que no existe en el esquema declarado.
- **Frecuencia de alucinación**: proporción de consultas que contienen al menos una alucinación de esquema.

## Reproducir un experimento

```bash
cd Implementaciones
uv run python evaluation/run_experiment_01.py
uv run python evaluation/run_experiment_02.py
uv run python evaluation/run_experiment_03.py   # requiere experiment_02 previo
uv run python evaluation/run_experiment_04.py
```

Los resultados se persisten en `evaluation/runs/<exp>_<timestampUTC>.json`. La carpeta está ignorada por git para no contaminar el historial con artefactos pesados; la trazabilidad queda garantizada por la combinación de `seed`, `model`, `corpus` y commit registrado en el JSON.
