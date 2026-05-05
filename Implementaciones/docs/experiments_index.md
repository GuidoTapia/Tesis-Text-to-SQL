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
| 05 | ¿el cierre estructural funciona? LLM emite IR vía tool use, ya no puede emitir SQL libre | 15 adv. + 5 control | 3 | sqlite3 | 3/3 alucinaciones atrapadas por verifier estructural antes de compilar; 0 prosa; 0 falsos positivos | 0,10 |
| 06 | ¿el prompt caching del schema baja el costo? | 15 adv. + 5 control | 3 | sqlite3 | implementación correcta pero la cuenta/tier no activa el caching; cache_creation y cache_read en 0 | 0,11 |
| 07 | ¿el cierre estructural funciona sobre IR de grafo? LLM construye queries PGQ vía tool use | 10 adv. + 5 control | grafo `social_graph` | DuckDB+DuckPGQ | 5/10 adversariales atrapadas por verifier; 5/5 controles limpios; 1 modo de fallo nuevo identificado (PathPattern multihop incoherente) | 0,09 |
| 07b | ¿los modos de fallo del 07 son modelo-específicos? | 10 adv. + 5 control | grafo `social_graph` | DuckDB+DuckPGQ | con Sonnet 4.6: 2/10 adv atrapadas, 8/10 reformulan sin alucinar; 1 ctrl falla por restricción de DuckPGQ no contemplada por el verifier | 0,25 |
| 08 | ¿el bucle de feedback rescata fallos del exp 07? | 10 adv. + 5 control | grafo `social_graph` | DuckDB+DuckPGQ | 8/10 adv y 5/5 ctrl tienen éxito (vs 4/10 y 5/5 en exp 07); 2 adv quedan irrecuperables tras 3 iter | 0,12 |
| 09 | ¿el bucle de feedback rescata fallos relacionales? | 10 rescue + 10 control | sample exp 02 | DuckDB estricto | 2/10 rescue_candidates rescatados (los 8 restantes son data quality irrecuperable); 9/10 should_still_pass mantienen éxito | 0,18 |

## Configuración detallada

| Exp. | Script | Output JSON | Modelo | Seed | Motor | Notas |
|:---:|---|---|---|:---:|:---:|---|
| 01 | `evaluation/run_experiment_01.py` | `runs/experiment_01_*.json` | claude-haiku-4-5-20251001 | 42 | DuckDB 1.4.4 + ext. `duckpgq` | Sample muestreado; primera corrida tuvo bug de verificador (FPs por aliases) y bug del LLM (markdown fences) — ambos corregidos antes del número final |
| 02 | `evaluation/run_experiment_02.py` | `runs/experiment_02_*.json` | claude-haiku-4-5-20251001 | 42 | DuckDB 1.4.4 + ext. `duckpgq` | `temperature=0` para reproducibilidad; helpers compartidos vía `evaluation/_helpers.py` |
| 03 | `evaluation/run_experiment_03.py` | `runs/experiment_03_*.json` | (no llama al LLM) | 42 (heredado) | sqlite3 stdlib | Reutiliza exactamente las predicciones de la corrida 02 más reciente; aísla el efecto del motor |
| 04 | `evaluation/run_experiment_04.py` | `runs/experiment_04_*.json` | claude-haiku-4-5-20251001 | n/a (corpus fijo) | sqlite3 stdlib | Corpus fijo en `corpus/adversarial/spider_decoys.json`; 15 adversariales + 5 controles |
| 05 | `evaluation/run_experiment_05.py` | `runs/experiment_05_*.json` | claude-haiku-4-5-20251001 | n/a (corpus fijo) | sqlite3 stdlib | Mismo corpus que 04. LLM forzado a tool use con `input_schema = IR_TOOL_INPUT_SCHEMA` (~9 KB). Pipeline: tool_call → parse_ir → verify_ir → compile_query → execute_on_db |
| 06 | `evaluation/run_experiment_06.py` | `runs/experiment_06_*.json` | claude-haiku-4-5-20251001 | n/a (corpus fijo) | sqlite3 stdlib | Mismo flujo que 05 con `cache_control` en la definición del tool; el ahorro esperado no se materializó por limitación a nivel de cuenta/tier (ver lab notebook) |
| 07 | `evaluation/run_experiment_07.py` | `runs/experiment_07_*.json` | claude-haiku-4-5-20251001 | n/a (corpus fijo) | DuckDB+DuckPGQ in-memory | Corpus PGQ en `corpus/adversarial/pgq_decoys.json`; grafo `social_graph` con 3 vertex y 3 edge labels creado en el script; pipeline tool_call → parse_ir → verify_ir → compile_query → execute_on_graph_db |
| 08 | `evaluation/run_experiment_08.py` | `runs/experiment_08_*.json` | claude-haiku-4-5-20251001 | n/a (corpus fijo) | DuckDB+DuckPGQ in-memory | Mismo corpus que 07 con `core.feedback.answer_with_feedback`; max_iterations=3 |
| 09 | `evaluation/run_experiment_09.py` | `runs/experiment_09_*.json` | claude-haiku-4-5-20251001 | 42 | DuckDB estricto sobre Spider sqlite | Subset de 20 preguntas (10 rescue_candidates + 10 should_still_pass) seleccionado del JSON de exp 02; bucle con max_iterations=3 |

## Tiempo y consumo

| Exp. | Llamadas LLM | Tokens in | Tokens out | Tiempo | Costo USD |
|:---:|---:|---:|---:|---:|---:|
| 01 | 20 | (no instrumentado) | (no instrumentado) | ~30 s | ~0,01 |
| 02 | 100 | 36 697 | 4 996 | 111,2 s | 0,049 |
| 03 | 0 | 0 | 0 | ~3 s | 0,00 |
| 04 | 20 | 5 944 | 893 | 25,2 s | 0,008 |
| 05 | 20 | 94 964 | 7 231 | 44,5 s | 0,10 |
| 06 | 20 | 94 964 | 7 675 | 48,0 s | 0,11 |
| 07 | 15 | 80 701 | 8 592 | 48,5 s | 0,09 |
| 07b | 15 | 80 281 | 9 011 | 86,8 s | 0,25 |
| 08 | 15 | 119 124 | 12 803 | 89,7 s | 0,12 |
| 09 | 20 | 175 949 | 21 408 | 137,8 s | 0,18 |
| Total | 245 | 688 624 | 72 619 | ~625 s | ~0,92 |

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

### Experimento 05 — cierre estructural vía tool use

**Pregunta**. Si el LLM no puede emitir SQL libre y su única salida válida es una IR estructural (vía tool use de Anthropic con `input_schema = IR_TOOL_INPUT_SCHEMA`), ¿qué tasa de alucinación detectada estructuralmente alcanza el pipeline sobre el mismo corpus adversarial del experimento 04?

**Diseño**. Mismo corpus que el experimento 04 (`corpus/adversarial/spider_decoys.json`, 15 adversariales + 5 controles, mismas tres bases de Spider). El LLM ya no recibe instrucción de devolver SQL como texto: se le da un tool `submit_query` cuyo `input_schema` es el JSON Schema de la IR-SQL/PGQ. El SDK rechaza payloads que no respetan el schema. El payload aceptado se convierte a IR con `parse_ir`, se verifica con `verify_ir`, se compila con `compile_query` y se ejecuta sobre sqlite3.

**Pipeline y puntos de fallo**. Cada predicción atraviesa cinco etapas y queda registrada por la primera que falla: `llm_call`, `no_tool_call`, `parse`, `verifier`, `compile`, `execution`. Esa categorización permite separar fallos del modelo, fallos del schema, fallos de la IR estructural, fallos del compilador y fallos semánticos.

**Resultado**.

| Etapa donde se detuvo | Adversariales | Controles |
|---|---:|---:|
| (éxito completo, ejecuta) | 9 | 5 |
| `verifier` | 3 | 0 |
| `execution` | 3 | 0 |

Las tres alucinaciones que el verificador atrapó (`email` en concert_singer, `Transmission` en car_1, `racket_brand` en wta_1) son las primeras detecciones genuinas a nivel estructural en todo el proyecto. Los tres errores de ejecución son de data quality (UTF-8 mal codificado en `players.last_name`), no de la IR.

**Comparación con el experimento 04**.

| Aspecto | Exp 04 (LLM emite SQL) | Exp 05 (LLM emite IR vía tool) |
|---|---|---|
| Salida malformada (prosa, markdown) | 3/15 adversariales | 0/15 |
| Alucinaciones de nombre en la salida | 1/15 (atrapada por sqlite) | 3/15 (atrapadas por verifier) |
| Tasa de detección estática | 100% (1/1) — sin oportunidad real | 100% (3/3) — con oportunidad estructural |
| Falsos positivos sobre controles | 0/5 | 0/5 |
| Tokens de entrada totales | 5 944 | 94 964 |

**Lectura**. El cierre estructural del cap. 4 se materializa: el LLM no puede emitir SQL inválido porque no emite SQL — emite IR cuyo formato la SDK valida y cuyo contenido el verificador valida. Las alucinaciones igual ocurren (el modelo se compromete con un nombre concreto cuando antes podía esquivar con prosa) pero ahora son detectables al cien por ciento antes de la compilación. La métrica natural deja de ser "tasa de execution accuracy" y pasa a ser "fracción de la salida que sobrevive a la verificación estructural", que es lo que el cap. 4 promete como propiedad de soundness.

**Costo y consideración futura**. El costo por pregunta es 12× el del experimento 04 porque el `input_schema` (~9 KB) viaja en cada llamada. La técnica natural de mitigación es prompt caching de Anthropic, que reduciría el costo del schema a una fracción del primer hit; por simplicidad de la rebanada inicial, no se incluyó.

**Commit asociado**: [próximo commit] feat: experimento 05 con tool use sobre IR.

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
