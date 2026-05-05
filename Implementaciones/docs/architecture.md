# Arquitectura del pipeline text-to-SQL/PGQ

Documento de referencia del flujo end-to-end de la propuesta. Describe
las cinco etapas que componen el pipeline tal como está implementado en
`Implementaciones/`, sus inputs y outputs, las garantías que cada una
provee y los puntos de fallo donde el sistema reporta y se detiene. Es
el complemento operacional del documento `ir_design.md`, que describe
la pieza central de datos.

## 1. Visión general

El pipeline transforma una pregunta en lenguaje natural y un esquema
proyectado $\Sigma_{\mathrm{proj}}$ en un conjunto de filas resultado
ejecutado sobre la base de datos objetivo. Las cinco etapas son:

```
   Pregunta NL + Σ_proj
            │
            ▼
   ┌────────────────────────────────────┐
   │  1. Anclaje al esquema dual         │
   │     (Σ_proj formateado para LLM)    │
   └────────────────┬───────────────────┘
                    │
                    ▼
   ┌────────────────────────────────────┐
   │  2. Construcción de IR vía LLM      │
   │     (tool use con submit_query)     │
   └────────────────┬───────────────────┘
                    │
                    ▼  payload JSON
   ┌────────────────────────────────────┐
   │  3. Verificación estructural        │
   │     parse_ir → verify_ir            │
   └────────────────┬───────────────────┘
                    │
                    ▼  IR validada
   ┌────────────────────────────────────┐
   │  4. Compilación determinista        │
   │     compile_query → SQL/PGQ string  │
   └────────────────┬───────────────────┘
                    │
                    ▼
   ┌────────────────────────────────────┐
   │  5. Ejecución sobre el motor        │
   │     sqlite3 (Spider) | DuckDB+PGQ   │
   └────────────────┬───────────────────┘
                    │
                    ▼
                  Filas
```

Cada etapa puede fallar y dejar el pipeline en una etapa fallida
nombrada (`llm_call`, `parse`, `verifier`, `compile`, `execution`). El
nombre del fallo es información valiosa: separa fallos del modelo,
fallos del schema validado por la SDK de Anthropic, fallos estructurales
de la IR, fallos del compilador y fallos semánticos detectados al
ejecutar.

## 2. Etapa 1: anclaje al esquema dual

Recibe el esquema relacional y, opcionalmente, las declaraciones de
property graph asociadas. Produce una representación textual que el
modelo de lenguaje consumirá como contexto. La función
`evaluation/_helpers.projected_schema_as_prompt` formatea un
`ProjectedSchema` en un texto que enumera tablas con sus columnas y
tipos, y declara los grafos de propiedad con sus labels de vértice y
arista, junto con los backings relacionales y las claves declaradas.

El cap. 4 propone tres mecanismos para construir $\Sigma_{\mathrm{proj}}$:
representación enriquecida del esquema (extensión del M-Schema de
XiYan-SQL con descriptores de grafo), enlazado bidireccional al estilo
RSL-SQL, y recuperación de valores literales al estilo CHESS. La
implementación actual aplica el primer mecanismo y deja los otros dos
como trabajo futuro: el esquema textual que recibe el modelo es la
estructura completa, sin filtrado por relevancia y sin valores
recuperados por similitud aproximada. La consecuencia operativa es que
con esquemas grandes (BIRD) habrá que añadir la fase de filtrado para
mantener el prompt en tamaños tratables.

## 3. Etapa 2: construcción de IR por el modelo de lenguaje

El modelo recibe un mensaje de sistema con las reglas operativas, un
mensaje de usuario con el esquema y la pregunta, y una declaración de
tool `submit_query` cuyo `input_schema` es el JSON Schema completo de
la IR (`core/ir/json_schema.IR_TOOL_INPUT_SCHEMA`). El parámetro
`tool_choice` fuerza la invocación del tool: el modelo no tiene la
opción de devolver texto libre.

La SDK de Anthropic valida el payload del tool contra el JSON Schema
antes de devolverlo. Esto es la primera red de seguridad: payloads que
violan el schema son rechazados por el servicio sin llegar al pipeline.
La segunda red es estructural: el formato del schema usa `oneOf` con
discriminador `type` para uniones como `Query`, `Expression` y
`FromExpr`, lo que reduce la superficie de payloads sintácticamente
válidos pero estructuralmente incoherentes.

Empíricamente se observó un patrón con Claude Haiku donde el modelo
ocasionalmente envuelve la IR como string JSON dentro del campo `query`
en lugar de como objeto anidado. La SDK acepta el patrón porque un
string es un valor JSON válido en algunos contextos del schema. El
script del experimento aplica una defensa: si `payload["query"]` es un
string, se decodifica con `json.loads` antes de pasarlo al parser.

## 4. Etapa 3: verificación estructural

`parse_ir` recibe el payload (un dict de Python decodificado del JSON)
y construye la instancia de IR tipada. Es la segunda red de seguridad:
detecta payloads con campos inválidos para la dataclass, tipos de nodo
desconocidos o coerciones imposibles. Los errores elevan
`IRParseError`.

`verify_ir` recibe la IR y el `ProjectedSchema` y aplica las cinco
clases de chequeo descritas en `ir_design.md` §7. Devuelve una lista
posiblemente vacía de `VerificationError`. Lista vacía implica que la
IR es estructuralmente válida respecto al esquema; lista no vacía
implica que se detectó al menos una de las clases de fallo conocidas.

La política operativa es acumular todos los errores antes de devolver,
no cortar al primer fallo. Si el verificador devuelve errores, el
pipeline no avanza a compilación: la consulta no llega al motor. Es la
materialización del cierre estructural prometido por el cap. 4.

## 5. Etapa 4: compilación determinista

`compile_query` recibe la IR validada y devuelve el SQL textual. La
operación no consulta estado externo y no realiza optimización. Por
construcción, dos IRs estructuralmente idénticas producen el mismo
string SQL.

La salida cumple las restricciones operativas de DuckPGQ documentadas
(variable obligatoria en cada arista, omisión del `AS` antes del alias
de `GRAPH_TABLE`). El compilador puede fallar con `NotImplementedError`
sólo si la IR contiene un nodo todavía no soportado (window functions,
ciertas variantes de CTE). Estos casos son raros y están documentados
como limitaciones explícitas.

## 6. Etapa 5: ejecución sobre el motor

El SQL compilado se ejecuta sobre uno de dos motores, según el contexto.

Para evaluación sobre Spider (consultas puramente relacionales), el
ejecutor es el módulo `sqlite3` de la stdlib. La elección está motivada
en `lab_notebook.md` §2026-05-01: SQLite es el motor que la literatura
de Spider usa por convención, lo que hace los números directamente
comparables; DuckDB en modo estricto rechaza valores que SQLite tolera
y produce penalidades de tipo no atribuibles al modelo.

Para consultas con bloques de grafo, el ejecutor es DuckDB con la
extensión `duckpgq` cargada en una conexión efímera donde el property
graph está declarado por código. Es el único motor en el ecosistema
actual que implementa SQL/PGQ y por tanto es el target operativo del
proyecto.

La función `evaluation/_helpers.execute_on_db` selecciona el motor por
parámetro `engine` y devuelve una tupla `(ok, error)` donde `ok` es un
booleano y `error` es el mensaje del motor cuando la ejecución falla.
La interfaz uniforme entre los dos motores es deliberada: simplifica el
código del experimento y permite comparaciones directas (como las
realizadas en el experimento 03).

## 7. Garantías del pipeline

El pipeline provee cuatro garantías operativas, derivadas de las
invariantes descritas en `ir_design.md` §6.

**Soundness referencial.** Si una IR pasa el verificador estructural,
toda referencia a tabla, columna, label de vértice, label de arista o
propiedad en la IR existe en $\Sigma_{\mathrm{proj}}$. La consecuencia
es que las alucinaciones de nombre del modelo de lenguaje no llegan a
la fase de compilación. Validada experimentalmente con tasa cien por
ciento sobre los casos donde el LLM produjo una alucinación detectable
(experimentos 05 y 07).

**Determinismo del compilador.** Dada una IR fija, el SQL emitido es
el mismo en cada invocación. La consecuencia operativa es que las
diferencias entre corridas se atribuyen exclusivamente al modelo
(varianza residual) o a los datos (calidad de la base), nunca al
compilador.

**Cierre estructural.** El modelo no puede emitir SQL inválido por
construcción porque su única salida válida es un payload IR cuyo formato
la SDK valida y cuyo contenido el verificador valida. Cuatro guardas
en serie reducen el universo de fallos posibles a errores semánticos
no detectables en estática.

**Especificidad sobre controles.** Sobre el sample evaluado, el
verificador no produjo falsos positivos en consultas correctas
(experimento 04: cinco de cinco; experimento 02: cero falsos positivos
sobre cien consultas). La excepción es ctrl-05 con Sonnet 4.6 sobre
exp 07b, que el verificador atrapa correctamente como restricción
operativa de DuckPGQ aunque el reporte automático lo cuente como falso
positivo (ver `lab_notebook.md` §rebanada nueve).

## 8. Mapa de los componentes

Los componentes del pipeline corresponden a módulos del repositorio
con responsabilidades acotadas.

| Componente | Módulo | Responsabilidad |
|---|---|---|
| Esquema relacional | `core/ir/schema.py` | Modelo de tablas, columnas, tipos |
| Esquema de grafo | `core/ir/schema.py` | Modelo de property graph y Σ_proj |
| Nodos de la IR | `core/ir/nodes.py` | Catálogo completo de dataclasses inmutables |
| Lifter | `core/ir/lift.py` | Conversión sqlglot AST → IR (testing y baselining) |
| Compilador | `core/ir/compile.py` | Conversión IR → SQL/PGQ string |
| Parser JSON | `core/ir/parse.py` | Conversión bidireccional dict ↔ IR |
| JSON Schema | `core/ir/json_schema.py` | Schema draft 2020-12 para tool use |
| Verificador estructural | `core/verifier/structural.py` | Cinco clases de chequeo sobre IR |
| Tipos de error | `core/verifier/errors.py` | Catálogo de kinds canónicos |
| Verificador MVP (baseline) | `core/verifier/static.py` | Versión inicial sobre sqlglot, mantenida para paridad |
| Helpers de experimento | `evaluation/_helpers.py` | Generación, ejecución, formateo de prompt, persistencia |
| Experimentos | `evaluation/run_experiment_*.py` | Scripts de cada corrida con su corpus y métricas |

## 9. Resumen de los experimentos

Los nueve experimentos ejecutados (incluyendo 07b con Sonnet) cubren
distintos aspectos del pipeline. La tabla maestra está en
`experiments_index.md`. Tres conclusiones cuantitativas para citar:

| Métrica | Valor | Origen |
|---|---:|---|
| Cobertura de la IR sobre Spider dev | 99% (99/100) | exp 02, round-trip estructural |
| Execution accuracy comparable con literatura | 98% | exp 03, sqlite3 sobre Spider |
| Tasa de detección estructural de alucinaciones genuinas | 100% | exp 05 (3/3) y exp 07 (6/6 tras rebanada 8) |

Tres hallazgos cualitativos sustantivos:

- El cierre estructural funciona: con tool use sobre IR, el LLM no puede
  emitir SQL inválido por construcción (exp 05).
- La utilidad del verificador disminuye en frecuencia, no en valor,
  cuando el modelo es más capaz (Sonnet 4.6 vs Haiku 4.5 en exp 07b).
- Modos de fallo del motor distintos del lenguaje (DuckPGQ rechaza
  vertex sin label, edges sin variable) requieren chequeos específicos
  separados conceptualmente del diseño abstracto del cap. 4.

## 10. Estado y trabajo futuro

El pipeline está implementado y validado experimentalmente sobre las
dimensiones relacional, de grafo e híbrida. Las cuatro piezas
sustantivas que quedan como trabajo futuro inmediato son:

1. **Verificación dinámica.** Implementar el bucle de retroalimentación
   estructurada del cap. 4 §4.5.4: cuando una IR pasa la verificación
   estática pero la ejecución falla, traducir el error del motor a un
   descriptor categórico y reinyectarlo al modelo como contexto adicional
   para una nueva iteración. Es la pieza más sustantiva y la que cierra
   el ciclo completo de la propuesta.

2. **Escalado del corpus PGQ.** El corpus actual de quince preguntas
   sobre el grafo `social_graph` valida la arquitectura pero es chico
   para conclusiones estadísticamente sólidas. Un corpus de cincuenta
   a cien preguntas, con categorías más finas y casos de uso
   industriales, daría un sample comparable al utilizado en literatura
   de SQL relacional.

3. **Filtrado del esquema previo a la generación.** Los mecanismos del
   cap. 4 §4.5.1 (enlazado bidireccional al estilo RSL-SQL, recuperación
   de valores al estilo CHESS) están declarados en el diseño pero no
   implementados. Para BIRD, donde los esquemas son grandes, esta fase
   es necesaria para mantener el prompt en tamaños tratables.

4. **Prompt caching del schema.** La implementación está hecha
   (experimento 06) pero el ahorro no se materializa en la cuenta
   actual por una limitación del tier de API. Cuando se rote la key, una
   sola corrida confirmará el orden de magnitud del ahorro y permitirá
   escalar los experimentos adversariales sin restricciones de costo.

Las cinco extensiones naturales del verificador y del compilador
documentadas en `ir_design.md` §10 son trabajo de menor envergadura y se
incorporan a medida que los experimentos las exhiban como necesarias.
