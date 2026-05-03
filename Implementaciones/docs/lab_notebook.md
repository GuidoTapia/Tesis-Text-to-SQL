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

---

## 2026-05-01

### Reproducibilidad con LLMs

Durante el refactor se observó que dos corridas idénticas del experimento, con la misma seed para el muestreo, devolvían números levemente distintos (un fallo de ejecución se movía entre corridas). El origen es la varianza del modelo: la API de Anthropic, sin una temperatura explícita, muestrea con cierta aleatoriedad incluso con prompts idénticos. Se fijó `temperature=0` en la llamada del helper compartido para reducir esta varianza. La reproducibilidad bit a bit no está garantizada (queda varianza residual del propio servicio), pero los números volvieron a ser estables entre corridas consecutivas en este sample.

### Refactor de helpers compartidos

Se extrajeron las funciones reusables de `run_experiment_01.py` a un módulo `evaluation/_helpers.py` (extracción de SQL del envoltorio markdown, formateo de esquema, llamada al LLM, ejecución sobre DuckDB y persistencia de resultados). El experimento 01 fue actualizado para consumirlas y se verificó que los números resultantes coincidían con los de la corrida previa al refactor. La intención del prefijo guion-bajo es marcar que el módulo es interno a `evaluation/` y no API pública.

### Paso 5b — segundo experimento medible (100 preguntas, 6 bases)

Se escaló el experimento a 100 preguntas distribuidas en 6 bases de Spider de complejidad y forma variadas: `concert_singer`, `world_1`, `car_1`, `wta_1`, `dog_kennels` y `student_transcripts_tracking`. El script vive en `evaluation/run_experiment_02.py` y comparte el helper común. Costo total observado: 36 697 tokens de entrada y 4 996 de salida (≈ USD 0,05) en 111 segundos de ejecución secuencial.

Resultados crudos: 100 consultas, 90 ejecutan sin error, 10 fallan, 0 detectados por el verificador, 0 falsos positivos.

La taxonomía manual de los diez errores resultó más informativa que el agregado: ninguno es alucinación de esquema. Tres son errores semánticos genuinos del LLM (incompatibilidad de tipos en `IN`, columnas no agregadas en `GROUP BY`); siete son problemas de calidad de datos del propio Spider en DuckDB (cinco errores `Conversion Error: invalid date field format ""` por filas con `birth_date` vacío en `players`, y dos errores de `Mismatch Type` por enteros que en SQLite admiten string vacío). La base `wta_1` concentra 7 de los 10 fallos por esta razón, no por dificultad intrínseca de las preguntas.

Conclusiones operativas para futuras corridas:

Primero, antes de ejecutar más experimentos sobre Spider conviene decidir cómo se trata la data sucia en DuckDB: cargar todas las columnas como `VARCHAR` con `sqlite_all_varchar=true`, preprocesar las bases para reemplazar valores inválidos, o aceptar la pérdida y reportarla. Cualquiera de las tres es defendible; lo que no se puede es comparar nuestros números con los de otras publicaciones que usan SQLite directamente sin documentar esta diferencia.

Segundo, el verificador estático sigue sin tener oportunidad de demostrar utilidad: en 100 preguntas con Claude Haiku 4.5 no apareció una sola alucinación de tabla o columna. Los modelos fuertes con esquemas medianos no son el escenario donde este tipo de verificación rinde. Las opciones realistas son evaluar con un modelo más débil, evaluar sobre BIRD (esquemas mucho más grandes), o construir un pequeño conjunto adversarial de preguntas con señuelo de esquema para medir capacidad sin depender de baseline.

Tercero, Claude Haiku 4.5 obtiene 90% de execution accuracy zero-shot en este sample, por encima del 80–85% típico reportado para baselines similares en Spider dev. El número no es estrictamente comparable por el problema de data quality recién mencionado, pero sirve como ancla.

### Stress de la composición híbrida

Antes de avanzar a la rebanada cinco se evaluó la eficacia del verificador estructural sobre el caso híbrido canónico, donde el resultado de un GRAPH_TABLE participa en un JOIN con una tabla relacional. La preocupación legítima es que el verificador, aunque cubre las tres clases declaradas en el cap. 4, podría fallar en queries que mezclan scopes de grafo y relacional en una misma cláusula.

Se extendió el fixture de DuckDB para incluir columnas de propiedad (age, country) en la tabla Person, agregando una dimensión que solo existe del lado relacional y que el JOIN tiene que poder usar. Sobre ese fixture se construyó por mano el caso canónico: un MATCH expone src, dst e id de la fuente; el JOIN enlaza el id con Person.id; el WHERE filtra por p.age > 25. La query ejecuta correctamente en DuckDB y devuelve la fila esperada.

Sobre el verificador se diseñaron cinco tests de stress. El primero confirma que la query híbrida válida no produce errores; los tres siguientes mutan deliberadamente la query para introducir alucinaciones del lado relacional (p.salary), del lado del alias del grafo (g.nonexistent) y del lado de las propiedades del vértice dentro del MATCH (a.email), y verifican que el verificador detecta cada una con el mensaje correcto. El último confirma que el WHERE externo puede referenciar columnas declaradas en COLUMNS del bloque GRAPH_TABLE sin generar falsos positivos.

Con esto queda demostrado que el sistema de scopes encadenados del verificador resuelve correctamente las tres procedencias de un identificador en una query híbrida (alias relacional, alias del bloque de grafo, variable de vértice ligada dentro del MATCH) y que la verificación es ejecutable sobre composiciones más complejas que la pareja relacional pura plus pareja graph pura.

### Rebanada cinco — el LLM emite IR vía tool use

Se cerró el ciclo prometido por el capítulo cuatro. El experimento cinco corre el mismo corpus adversarial del experimento cuatro (quince preguntas adversariales más cinco controles) pero el LLM ya no emite SQL libre: invoca el tool ``submit_query`` con un payload que valida contra el JSON Schema de la IR. La SDK rechaza payloads que no respetan el schema, el parser convierte el dict a IR tipada, el verificador estructural opera sobre la IR, el compilador determinista la materializa a SQL y finalmente se ejecuta contra sqlite3.

La construcción del JSON Schema requirió dos iteraciones. La primera versión usaba sintaxis de tuple de items (``"items": [a, b]``) heredada de drafts anteriores y un enum que mezclaba string con null, ambos rechazados por la validación draft 2020-12 que exige Anthropic. La segunda versión sustituye los tuples por ``prefixItems`` y separa los enums de null en oneOfs. Tras el ajuste el SDK acepta el schema y el modelo respeta el contrato.

La primera corrida con el schema corregido reveló otro problema sutil. Claude Haiku, ante un schema con ``oneOf`` donde uno de los miembros es un objeto complejo, frecuentemente envuelve el contenido como string JSON dentro del campo ``query`` en vez de como objeto anidado. El SDK no detecta este patrón porque técnicamente un string es un valor JSON aceptable en algunos contextos del schema. Se agregó al script una defensa que detecta cuando ``query`` llega como string y lo decodifica con ``json.loads`` antes de pasarlo al parser. También se reescribió el system prompt agregando una instrucción explícita en mayúsculas y un ejemplo concreto de invocación correcta, lo que reduce notablemente la frecuencia del wrapping incorrecto.

Resultados sobre las quince adversariales y cinco controles. El verificador estructural atrapó tres alucinaciones genuinas de columna que el LLM intentó usar concretamente en la IR: ``email`` en concert_singer, ``Transmission`` en car_1 y ``racket_brand`` en wta_1. Las tres tienen el mismo perfil: el LLM se comprometió con un nombre concreto (no podía esquivar con prosa o NULL placeholder porque el contrato del tool exige una IR completa) y el verificador detectó la inexistencia del nombre antes de cualquier compilación. Sobre los cinco controles el verificador no produjo ningún falso positivo. Tres adversariales de wta_1 que pasaron por el verificador fallaron en ejecución: las tres tienen el mismo origen, un caracter UTF-8 mal codificado en la columna ``last_name`` que sqlite3 no puede decodificar; es data quality, no alucinación.

La comparación con el experimento cuatro es la pieza más informativa. En el experimento cuatro, donde el LLM emitía SQL libre, sólo una de las quince adversariales produjo una alucinación verdadera de nombre; tres respondieron en prosa rechazándose a inventar y el resto encontró reformulaciones creativas. En el experimento cinco, donde el LLM emite IR estructural, no hay opción de prosa, no hay opción de markdown fences, y la frecuencia de alucinación detectable subió de uno a tres sobre quince. Más importante que el número absoluto es la propiedad: en el experimento cuatro el verificador no podía actuar sobre las respuestas en prosa porque ni siquiera eran SQL parseable; en el experimento cinco las tres alucinaciones que ocurrieron fueron detectadas con cien por ciento de tasa por el verificador estructural antes de compilar.

El costo del experimento cinco fue significativamente mayor que el del cuatro, en torno a noventa y cinco mil tokens de entrada contra cinco mil novecientos del cuatro. El factor principal es que el JSON Schema de la IR pesa cerca de nueve KB y se incluye en cada llamada como input_schema del tool. Esto se podría amortizar más adelante con prompt caching de Anthropic, técnica que cachea el schema y reduce el costo a la fracción correspondiente al prompt incremental por pregunta. Para esta rebanada se prefirió la simplicidad de no introducir caching todavía y reportar el costo nominal.

Lectura para la tesis. El cierre estructural del capítulo cuatro se materializa: el LLM no puede emitir SQL inválido por construcción porque su única salida válida es una IR cuyo formato la SDK valida y cuyo contenido el verificador estructural valida. Las alucinaciones de nombres que el modelo igual produce, ahora porque está obligado a comprometerse con un nombre, las atrapa el verificador con tasa cien por ciento sobre el sample evaluado. La utilidad del verificador no es retórica: cuando el régimen de generación lo expone a casos donde puede actuar, actúa.

### Rebanada seis — prompt caching del schema (resultado parcial)

Se implementó la optimización natural sugerida por el experimento cinco: marcar la definición del tool con ``cache_control={"type": "ephemeral"}`` para que el JSON Schema de la IR (alrededor de tres mil quinientos tokens) viaje al servidor solo en la primera llamada y se recupere del cache en las subsiguientes, pagando el diez por ciento del costo nominal por hit. La implementación vive en evaluation/run_experiment_06.py, idéntica al experimento cinco salvo por el cache_control y el reporte separado de tokens regulares, de creación de cache y de lectura de cache.

La corrida del experimento sobre las veinte preguntas no activó el caching: tanto cache_creation_input_tokens como cache_read_input_tokens son cero en todas las invocaciones, mientras que input_tokens regular se mantiene idéntico al experimento cinco. Se diagnosticó la causa con tres pruebas adicionales: una con cache_control en el tool y schema completo, otra con cache_control en el system prompt artificialmente extendido por encima del mínimo de Haiku de dos mil cuarenta y ocho tokens, y una tercera agregando el header beta explícito anthropic-beta=prompt-caching-2024-07-31. Las tres devolvieron exactamente los mismos contadores en cero. La SDK acepta el cache_control sin protestar, el campo Usage tiene los slots correspondientes, pero el motor de inferencia no marca tokens como cacheados.

El comportamiento es consistente con una limitación de la cuenta o del service_tier asociado a la API key actual; el campo inference_geo aparece como not_available en las respuestas, lo que sugiere que la cuenta está en una variante de tier estándar sin caching habilitado. La implementación queda correcta y reusable: cuando se rote la key a una con caching, una sola corrida producirá las métricas esperadas sin cambiar el código. Se anotó en el script una advertencia visible al final del reporte cuando se detecta cache=0, para que la limitación quede declarada en cada corrida sin requerir lectura del lab notebook.

Lectura para la tesis. La rebanada seis no logra el ahorro de costo prometido sobre esta cuenta, pero el resultado no invalida el diseño: el costo del experimento cinco (USD diez centavos por veinte preguntas) ya es bajo en términos absolutos. Si el caching se llegara a activar en futuras corridas, el costo unitario caería un orden de magnitud y permitiría escalar el experimento adversarial a corpus de cien o doscientas preguntas sin volverlo presupuestariamente significativo. La rebanada se da por cerrada en el sentido de implementación; la medición del ahorro queda marcada como pendiente operativa, no como contradicción metodológica.

### Rebanada siete — el LLM construye consultas PGQ vía IR

Se cerró el ciclo sobre la dimensión grafo. El experimento siete corre el mismo pipeline que el cinco pero el esquema disponible para el LLM ahora incluye un property graph y las preguntas requieren bloques MATCH para responderse.

Diseño del esquema. Se construyó un grafo de prueba pequeño pero expresivo, ``social_graph``, con tres labels de vértice (Person, City, Company) y tres labels de arista (knows entre dos Person, lives_in de Person a City, works_at de Person a Company). El grafo se declara tanto como ``ProjectedSchema`` para que el verificador lo conozca como en DuckDB con la sintaxis CREATE PROPERTY GRAPH para que las consultas compiladas se puedan ejecutar. Person tiene cuatro instancias (Alice, Bob, Carol, Dave), tres ciudades y dos empresas, con aristas que componen una red social mínima.

Diseño del corpus. Se escribieron quince preguntas en ``corpus/adversarial/pgq_decoys.json``, diez adversariales y cinco controles. Las adversariales cubren cuatro categorías nuevas específicas de la dimensión grafo: ``missing-vertex-label`` (preguntas que invitan a inventar un label como Manager o Car), ``missing-edge-label`` (relaciones inexistentes como reports o hates), ``missing-vertex-property`` (propiedades como email o salary no presentes en el backing table) y ``missing-relational-column`` (queries híbridas que aluden a columnas inexistentes desde el lado relacional).

Primera corrida. El verificador rechazó los cinco controles con falsos positivos del tipo ``unknown_qualifier``. La inspección del payload mostró que Haiku 4.5 emitía consistentemente IR estructuralmente inválida: omitía la cláusula ``columns`` del MatchPattern y referenciaba variables de vértice (``b``, ``c``, ``p``) directamente desde el outer SELECT. El verificador hacía lo correcto al rechazar; el problema era que el modelo no entendía el contrato de scope entre el bloque MATCH y el RelationalQuery contenedor.

Iteración del prompt. Se reescribió el system prompt con una nueva regla numerada que explicita el contrato de scope: las variables de vértice y arista son visibles solo dentro del propio MATCH (en where y columns), no se propagan al outer scope, y para que el outer SELECT acceda a propiedades hay que proyectarlas vía COLUMNS y referenciarlas como ``alias_del_FromGraphMatch.alias_de_la_columna``. El ejemplo del prompt se reescribió para mostrar explícitamente el patrón ``COLUMNS (b.name AS friend) ... outer SELECT g.friend`` en lugar del ``SELECT *`` que tenía antes.

Segunda corrida con el prompt corregido. Cinco de cinco controles ejecutan limpiamente sin un solo falso positivo. Cinco de diez adversariales son atrapadas por el verificador estructural antes de cualquier compilación. Cuatro de diez adversariales pasan el pipeline completo: tres son comportamiento ``best effort`` trivialmente válido del modelo (``SELECT name FROM Person WHERE NULL``, ``SELECT id FROM Person WHERE FALSE``, ``SELECT * FROM Person WHERE name='Alice'``) ante preguntas que no tienen respuesta en el esquema y que el modelo elige no inventar; una es una reformulación creativa donde el modelo interpretó ``visited`` como ``lives_in``, lo que devuelve una fila válida pero arguablemente no responde a la pregunta original. Una de diez adversariales falla en ejecución: el modelo intentó construir un PathPattern multihop con la cadena ``Company-[lives_in]->City``, sintácticamente válida pero semánticamente incoherente porque el grafo declara ``lives_in`` de Person a City, no de Company a City. DuckDB la atrapó al ejecución; el verificador estructural no la detectó.

Hallazgo de extensión del verificador. La falla de adv-08 expone una clase de chequeo que el verificador actual no implementa: la coherencia source→edge→destination en PathPatterns multihop. El verificador chequea que cada label de vértice y de arista exista por separado, pero no verifica que el destination_label declarado en una arista corresponda al label del siguiente vértice en el camino. Esta sería una cuarta clase de chequeo natural a agregar, complementaria a las tres del capítulo cuatro y particularmente relevante para queries de grafo con caminos largos.

Lectura para la tesis. La rebanada siete demuestra que el cierre estructural del cap. 4 funciona también sobre la dimensión grafo, no solo sobre la relacional. Los cinco de diez adversariales atrapados por el verificador y los cero falsos positivos sobre los cinco controles son números honestos que sostienen la propiedad de soundness sobre el caso de grafo. El hallazgo del modo de fallo en adv-08 (PathPattern multihop incoherente) es valor adicional: identifica una extensión concreta y ejecutable del verificador que el cap. 4 no había explicitado y que esta rebanada motiva como trabajo futuro inmediato.

### Paso 5c — segundo motor de ejecución y línea base comparable

Se eligió la opción D para resolver la cuestión del manejo de data sucia en Spider: agregar un segundo backend de ejecución basado en `sqlite3` de la stdlib y dejar DuckDB para SQL/PGQ donde el motor estricto es necesario y la data es nuestra. El verificador queda intacto porque opera sobre el SQL y el esquema, no sobre el motor.

Se extendió `evaluation/_helpers.execute_on_db` para aceptar un parámetro `engine` que despacha a `_execute_duckdb` (default, comportamiento previo preservado) o a `_execute_sqlite` (nuevo). Los experimentos 01 y 02 siguen usando DuckDB sin cambios. Se construyó `evaluation/run_experiment_03.py`, que carga las predicciones del experimento 02 más reciente y las re-ejecuta sobre `sqlite3` sin volver a llamar al LLM. Esta decisión metodológica aísla el efecto del motor: las predicciones son idénticas, lo único que varía es quién las ejecuta.

Resultado: DuckDB ejecuta 90 de 100 consultas; sqlite3 ejecuta 98. Las 8 consultas que solo fallan en DuckDB son exactamente las que se sospechaba (campos de fecha vacíos en `wta_1.players.birth_date`, string `'null'` en `cars_data.Horsepower`, `player_id` vacío en `wta_1`). En 0 consultas ocurre lo opuesto, lo que confirma que DuckDB en este contexto es un superset de chequeos respecto de SQLite. Solo 2 fallos sobreviven en ambos motores y son errores semánticos genuinos del LLM (`GROUP BY` violado, comparación de tipos en `IN`).

Implicaciones para los reportes futuros: la línea base comparable con la literatura es **98% de execution accuracy de Claude Haiku 4.5 en este sample de Spider dev**. El número en DuckDB queda como métrica interna útil para el pipeline PGQ. Reportar ambos en el capítulo de evaluación, siempre con la advertencia de cuál es la métrica relevante para qué comparación.

### Paso 5d — primer experimento adversarial sobre el verificador

Con la línea base resuelta, se construyó un corpus pequeño y trackeado en `corpus/adversarial/spider_decoys.json` con quince preguntas adversariales y cinco controles, distribuidas en tres bases (`concert_singer`, `car_1`, `wta_1`). Las preguntas adversariales están construidas a mano para inducir al LLM a inventar tablas o columnas inexistentes; cubren cuatro categorías declaradas en el archivo: `missing-column`, `missing-table`, `decoy-rename` e `implicit-attribute`. Las controles son directamente respondibles con el esquema y existen para medir falsos positivos del verificador. El experimento `evaluation/run_experiment_04.py` corre el flujo de generación y verificación sobre el corpus usando el motor sqlite3 (laxo, evita el ruido de tipado estricto observado en entradas anteriores).

Los números planos del experimento sugieren a primera vista una tasa de alucinación bajísima: una sola pregunta adversarial de quince produjo un error `no such column` en sqlite3, y el verificador la atrapó. La especificidad fue perfecta, con cero falsos positivos sobre cinco controles. La clasificación gruesa "el LLM alucinó / no alucinó" oculta sin embargo el comportamiento real del modelo, que se revela inspeccionando las predicciones una por una.

Lo que efectivamente ocurrió en las quince adversariales es más matizado. En tres casos Claude se rehusó a inventar y devolvió texto en lenguaje natural explicando que la información no está en el esquema; el verificador rechazó esa salida como `parse_error` y sqlite3 falló con un error de sintaxis, ambos comportamientos correctos pero por razones distintas a las previstas. En unos seis casos Claude produjo SQL válido reformulando la pregunta con columnas semánticamente cercanas: usó `Country` cuando se le preguntó por `nationality`, devolvió `Song_Name` cuando se le preguntó por género, y aplicó `Is_male = 0` para responder por mujeres. En dos casos hizo un pivote conservador, devolviendo `hand` (mano dominante, L/R) cuando se le preguntó por género, o usando `NULL as coach_name` para llenar una columna que no podía obtener. Una pregunta adversarial resultó tener respuesta válida que yo no había anticipado: aunque `players` no tiene altura, `matches.winner_ht` y `matches.loser_ht` sí, y Claude las encontró; ese es un error de diseño del corpus, no del modelo. Solo en un caso, `car_1-adv-03` ("colors of cars made in 1980"), el LLM cayó en la trampa y escribió `SELECT DISTINCT Color FROM cars_data`; el verificador detectó `unknown_column: Color` correctamente.

Este resultado es importante para el encuadre del capítulo de evaluación. La hipótesis implícita del verificador era que los LLMs alucinan con frecuencia y que esa frecuencia justifica una capa de verificación previa a la ejecución. Para Claude Haiku 4.5 con prompt de esquema explícito, esa hipótesis se sostiene mucho menos de lo esperado: el modelo prefiere reformular, devolver NULL o rehusarse antes que inventar nombres. La contribución del verificador se entiende mejor entonces como una **garantía estructural de soundness** que como una métrica de frecuencia. Cuando produce SQL que pasa la verificación, ese SQL referencia exclusivamente nombres del esquema, y eso vale tanto para las quince adversariales como para las cien preguntas aleatorias del experimento 02. La métrica natural del enfoque deja de ser "tasa de detección de alucinaciones" (que en este régimen es casi indistinguible del cero) y pasa a ser una propiedad cualitativa más cercana a un type-checker que a un detector estadístico.

Pendientes que el experimento dejó visibles. Refinar el corpus retirando los casos donde Claude encontró un workaround válido y agregando preguntas que fuercen al modelo a comprometerse con un nombre específico. Probar con un modelo más débil donde la tasa de alucinación sí sea no trivial. Revisar la calibración del prompt para evitar las respuestas en prosa que ahora aparecen como `parse_error`, o aceptar esa señal como otra forma legítima de "el LLM no produjo SQL válido" y reportarla por separado. Cualquiera de los tres caminos amerita una sesión propia.
