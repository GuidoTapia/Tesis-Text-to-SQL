# Diseño de la representación intermedia IR-SQL/PGQ

Documento de referencia técnica del componente central de la propuesta de
tesis. Describe la representación intermedia tipada IR-SQL/PGQ tal como está
implementada en `core/ir/`, sus invariantes, su relación con las
representaciones previas del estado del arte, las clases de verificación
estructural que admite, su compilación determinista a SQL/PGQ ejecutable, y
su contrato de serialización JSON utilizado en la generación por el modelo.

Este documento complementa a `lab_notebook.md` (bitácora narrativa de la
implementación) y a `experiments_index.md` (índice tabular de los
experimentos realizados). Está pensado como insumo directo para el capítulo
cuatro de la tesis, en particular para la sección §4.5.2.

## 1. Posición de la IR en la arquitectura

La arquitectura propuesta encadena cinco etapas: anclaje al esquema dual
($\Sigma_{\mathrm{proj}}$), construcción de la IR-SQL/PGQ por el modelo de
lenguaje, verificación estructural sobre la IR, compilación determinista a
SQL/PGQ y ejecución sobre DuckDB con la extensión DuckPGQ. La IR es la
pieza que las cuatro etapas restantes producen, consumen, validan y
materializan.

Su valor en la arquitectura es triple. Primero, es el contrato fijo entre
el modelo de lenguaje y el resto del pipeline: el modelo no produce SQL
libre sino una instancia de IR cuyo formato está validado al momento de
generación. Segundo, es la única estructura sobre la cual el verificador
estructural opera; el verificador no parsea SQL ni infiere desde el AST de
sqlglot, sino que recorre la IR tipada y resuelve referencias contra el
esquema proyectado. Tercero, es la entrada del compilador determinista que
materializa la consulta concreta; dos instancias estructuralmente
idénticas de la IR producen el mismo SQL textual.

La consecuencia inmediata de esta posición es que la IR define el
vocabulario en el cual se expresan tanto la propuesta como sus
limitaciones. Cuando se afirma que el modelo no puede emitir alucinaciones
de esquema "por construcción" (cap. 4 §4.4 P2), lo que se quiere decir es
que la única salida válida del modelo es una instancia de IR cuya
estructura niega las alucinaciones que el verificador detecta antes de
compilar. Cuando se distingue entre verificación estática y dinámica, lo
que se está distinguiendo es lo que la IR sabe declarar de antemano contra
lo que solo se aprende al ejecutar.

## 2. Linaje y diferencias con representaciones previas

Tres familias de representaciones intermedias del estado del arte
informan el diseño.

**SemQL** (Guo et al., IRNet, 2019) introdujo la idea de descomponer la
generación de SQL en una secuencia de decisiones tipadas que un modelo
recurrente toma sobre un esqueleto. La granularidad es léxica: cada acción
expande un slot del esqueleto con un componente de la consulta. La
contribución central de SemQL es desacoplar la generación de la sintaxis
final del SQL de la decisión semántica del slot.

**NatSQL** (Gan et al., 2021) refinó la idea reduciendo la cantidad de
decisiones que el modelo debe tomar al colapsar varios constructos de SQL
en operaciones equivalentes pero más uniformes. La consecuencia es un
espacio de búsqueda menor y una cobertura empírica mejor sobre Spider y
BIRD. NatSQL es estrictamente un IR para SQL relacional.

**RESDSQL skeletons** (Li et al., 2023) tomó una dirección distinta:
producir primero un esqueleto de la consulta SQL (una sucesión de keywords
y placeholders) y luego rellenar los placeholders con un decoder
condicional. La separación skeleton-versus-relleno es operativa: facilita
la decodificación restringida y reduce errores de consistencia entre
cláusulas.

La IR-SQL/PGQ propuesta hereda principios de las tres familias y agrega
dos elementos que ninguna contempla. Hereda de SemQL y NatSQL la
estructura tipada que niega referencias a elementos no declarados; hereda
de RESDSQL la disciplina de mantener un compilador determinista
descendiente de la IR a la consulta textual; agrega un tratamiento
uniforme de bloques de grafo (`MatchPattern`, `PathPattern`,
`VertexPattern`, `EdgePattern`) y la composicionalidad híbrida
(`FromGraphMatch`) que permite que el resultado de una consulta de grafo
participe como tabla derivada en el contexto relacional. Estos dos
elementos son la novedad declarada del diseño.

## 3. Estructura general y mecánica

La IR es un árbol cuyos nodos están representados como
`@dataclass(frozen=True)` de Python 3.11. La inmutabilidad es deliberada
y tiene tres consecuencias prácticas: las instancias son hashables y por
tanto se pueden usar como claves en estructuras auxiliares y como
elementos de conjuntos; la igualdad estructural está garantizada por el
mecanismo nativo de dataclasses, sin necesidad de redefinir `__eq__`; y
no hay riesgo de aliasing accidental, ya que cualquier "modificación"
exige construir una nueva instancia mediante `dataclasses.replace`.

Las colecciones internas a un nodo (e.g., `RelationalQuery.select`,
`MatchPattern.patterns`) están tipadas como `tuple[..., ...]` en lugar de
`list[...]`. La elección refuerza la inmutabilidad y se traduce
directamente al formato JSON de serialización: cada tupla del IR aparece
como un array JSON.

El despacho por tipo se realiza vía `isinstance` o pattern matching de
Python 3.11. El patrón se observa tanto en `lift.py` (que mapea nodos de
sqlglot a nodos de la IR) como en `compile.py` (que mapea nodos de la IR
a fragmentos de texto SQL/PGQ) y en `verifier/structural.py` (que ejecuta
los chequeos por clase de nodo). Ningún nodo de la IR contiene lógica de
método: son contenedores puros de datos. La separación entre datos e
interpretación es deliberada y permite agregar nuevas operaciones (por
ejemplo, un visitor que infiere tipos) sin tocar el catálogo de nodos.

## 4. Los tres bloques de la IR

### 4.1 Bloque relacional

Cubre el subconjunto de SQL ANSI que aparece en los corpus de evaluación
relevantes para la tesis (Spider, BIRD). El bloque relacional se
estructura en cinco capas: referencias a esquema, expresiones, ítems de
cláusulas, fuentes de la cláusula `FROM` y consultas de nivel superior.

Las **referencias a esquema** son `TableRef(name, alias)` y
`ColumnRef(name, qualifier)`. La separación entre `name` y `qualifier` en
`ColumnRef` permite distinguir una columna desnuda (qualifier nulo) de
una columna calificada por un alias de tabla o por una variable de
vértice ligada en un bloque de grafo.

Las **expresiones** se organizan alrededor de una clase marcadora
`Expression` y un conjunto de subclases que cubren los casos canónicos:
literales tipados (`Literal`), referencias a columnas en posición de
expresión (`ColumnExpr`), comodines (`Star`), operadores binarios
(`BinaryOp`) que cubren tanto comparaciones como aritmética y lógica,
operadores unarios (`UnaryOp`), llamadas a funciones escalares
(`FunctionCall`) y de agregación (`Aggregate`, con marcador de
`DISTINCT`), expresiones condicionales (`CaseExpr`), conversiones de tipo
(`CastExpr`), patrones de comparación de cadenas (`LikeExpr`), pertenencia
en conjuntos o subconsultas (`InExpr`), pruebas de nulidad
(`IsNullExpr`), rangos (`BetweenExpr`), existenciales sobre subconsultas
(`ExistsExpr`), subconsultas como expresión (`Subquery`) y agrupación
explícita por paréntesis (`ParenExpr`). Todas las expresiones son
recursivas: sus campos pueden contener otras expresiones, lo que permite
representar combinaciones arbitrarias.

Los **ítems de cláusulas** son `SelectItem(expr, alias)` y
`OrderItem(expr, direction, nulls)`. La separación entre el ítem y la
expresión que contiene es importante: un mismo `Expression` puede
aparecer en posiciones distintas con metadatos distintos (por ejemplo, el
mismo `ColumnExpr` puede ser un ítem de SELECT con alias o un argumento
de una función agregada sin alias).

Las **fuentes** del FROM son `FromTable(table)`, `FromSubquery(query, alias)`
y `Join(left, right, kind, on, using)`. Estas tres clases componen una
unión `FromExpr` que admite anidamiento arbitrario: el lado izquierdo o
derecho de un `Join` puede ser otro `Join` (lo que permite expresar
joins de tres o más tablas), una `FromSubquery` (lo que permite
subconsultas en el `FROM`), o un `FromGraphMatch` (lo que permite
composición híbrida con bloques de grafo, ver §4.3).

Las **consultas de nivel superior** son `RelationalQuery` y
`SetOperation`. `RelationalQuery` agrupa todas las cláusulas canónicas de
una consulta SQL: `select`, `from_` (con guion bajo final para evitar
colisión con la palabra clave de Python), `where`, `group_by`, `having`,
`order_by`, `limit`, `offset` y un marcador `distinct`. `SetOperation`
representa `UNION`, `INTERSECT` y `EXCEPT` con sus variantes `ALL`. Una
unión `Query = RelationalQuery | SetOperation` es la raíz del árbol.

### 4.2 Bloque de grafo

Materializa la cláusula `GRAPH_TABLE` del estándar SQL/PGQ tal como la
implementa DuckPGQ. Se compone de cuatro nodos.

`VertexPattern(var, label)` representa un patrón de vértice en un MATCH.
La variable es obligatoria; el label puede ser nulo en el modelo de la
IR pero el verificador lo flagea como restricción operativa de DuckPGQ
(ver §7.5).

`EdgePattern(var, label, direction)` representa un patrón de arista. La
variable es obligatoria por restricción del motor: DuckPGQ rechaza
patrones con arista sin variable, incluso cuando la variable no se
referencia luego. La dirección admite tres valores literales: `->` para
aristas dirigidas hacia adelante, `<-` para aristas dirigidas hacia
atrás (lo que invierte los roles de source y destination en la
declaración del edge), y `-` para aristas no dirigidas que admiten
cualquier orientación.

`PathPattern(head, steps)` agrupa un vértice cabecera y una secuencia
ordenada de pasos, donde cada paso es un par `(EdgePattern, VertexPattern)`.
Los caminos multihop se expresan como `PathPattern` con dos o más pasos.

`MatchPattern(graph, patterns, where, columns)` es la raíz del bloque de
grafo. El campo `graph` referencia el nombre de un property graph
declarado en el catálogo de DuckPGQ. El campo `patterns` puede contener
uno o más `PathPattern` (multipattern MATCH del estándar). El campo
`where` admite cualquier `Expression` resuelta contra las variables del
MATCH. El campo `columns` lista los `SelectItem` que el bloque expone
hacia afuera; estos son la única superficie de las variables del MATCH
visible al `RelationalQuery` contenedor.

### 4.3 Composición híbrida

El nodo `FromGraphMatch(match, alias)` es el punto de unión entre los
bloques relacional y de grafo. Como elemento de la unión `FromExpr`,
puede ocupar cualquier posición que un `FromTable` ocuparía: ser la
única fuente de un `RelationalQuery` (caso de consulta puramente de
grafo), ser un operando de un `Join` (caso híbrido canónico, donde el
resultado del MATCH se mezcla con una tabla relacional), o ser un
operando de otro `Join` anidado (composiciones más profundas).

La composicionalidad es estructural: no hay nodos especiales para casos
híbridos. La unión `FromExpr` cierra el catálogo de fuentes y todas las
operaciones del bloque relacional (filtros, agrupaciones, ordenamientos,
agregaciones) siguen siendo expresables uniformemente sobre el resultado
del MATCH. Esta uniformidad es uno de los dos elementos que distinguen a
la IR-SQL/PGQ de las representaciones previas, todas concebidas para
SQL relacional puro.

El alias del `FromGraphMatch` cumple un rol específico en la
verificación. Las variables internas del MATCH (`a`, `b`, `k`, etc.) no
se propagan al scope contenedor; lo que sí se propaga son las columnas
declaradas en `MatchPattern.columns`, accesibles desde el outer
`RelationalQuery` mediante la sintaxis `<alias>.<columna>`. El
verificador estructural mantiene tres mapas separados (alias de tablas
relacionales, alias de bloques de grafo, variables de vértice del MATCH)
para resolver correctamente cada qualifier de columna a su origen.

## 5. Esquema proyectado

El componente del cap. 4 §4.5.1 tiene un correlato directo en el modelo de
datos del verificador. La clase `ProjectedSchema` agrega un
`RelationalSchema` (con `TableSchema` y `ColumnSchema`, ambos tipados
con el tipo declarado de la columna y un marcador opcional de clave
primaria) y una tupla de `PropertyGraphSchema`. Cada
`PropertyGraphSchema` declara su nombre, sus `PropertyGraphVertexTable`
(que enlazan un label de vértice con una tabla relacional y sus
columnas clave) y sus `PropertyGraphEdgeTable` (que enlazan un label de
arista con una tabla, los labels de source y destination y sus
respectivas columnas clave).

`ProjectedSchema` es el único insumo estructural que el verificador
consume sobre la base de datos. Esto es deliberado: el verificador no
mira los datos, no infiere tipos por contenido, no consulta el motor.
Toda su capacidad de detección depende de lo que el esquema declare. La
construcción del esquema es por tanto una decisión de modelado que
condiciona el alcance de la verificación; el cargador
`from_spider_tables` materializa esta construcción para los corpus
canónicos de evaluación.

## 6. Invariantes de la IR

Cuatro invariantes sostienen el comportamiento esperado del sistema y
son los que las pruebas y los experimentos validan empíricamente.

**Inmutabilidad e igualdad estructural.** La combinación de
`@dataclass(frozen=True)` con campos que son tuplas o tipos primitivos
garantiza que dos instancias con los mismos contenidos son iguales bajo
`==` y producen el mismo hash. La consecuencia operativa más visible es
que el compilador determinista produce el mismo string SQL para
instancias estructuralmente idénticas, sin que se necesite normalizar la
salida ni canonicalizar la entrada. Esta propiedad se invoca en la
sección §4.5.3 de la tesis como "determinismo estricto" del compilador.

**Round-trip de lift y compile.** Para todo `Query` $q$ producido por
`lift_sql` sobre un SQL textual $s$, la IR resultante de
`lift_sql(compile_query(q))` es estructuralmente idéntica a $q$ módulo
nodos `ParenExpr` cosméticos. La invariante es la versión empírica de
"el compilador es la inversa derecha del lifter". Está cubierta por
veintitrés tests unitarios y por un test de cobertura sobre las cien
predicciones del experimento dos, donde el round-trip exacto se observa
en noventa y nueve casos; el único caso no cubierto utiliza una window
function que la IR todavía no representa.

**Round-trip de parse y to_dict.** Para toda IR $q$ construida en
memoria, `parse_ir(to_dict(q))` es estructuralmente idéntica a $q$. La
invariante es la propiedad central del contrato JSON con el modelo de
lenguaje: cualquier IR que el modelo produzca y que pase la validación
del schema y del parser puede ser comparada por igualdad estructural con
la IR esperada por una prueba o por un benchmark.

**Soundness referencial bajo verificación exitosa.** Si $q$ es una IR que
pasa el verificador estructural contra $\Sigma_{\mathrm{proj}}$, entonces
toda referencia a tabla, columna, label de vértice, label de arista o
propiedad en $q$ corresponde a un elemento declarado en
$\Sigma_{\mathrm{proj}}$. La invariante es la formulación operativa de la
propiedad de soundness sobre nombres del esquema que el cap. 4 §4.5.4
declara. La condición inversa (completeness) no se garantiza: el
verificador puede dejar pasar consultas que el motor luego rechaza por
errores semánticos o de tipo no previstos; esos quedan para el régimen
de verificación dinámica.

## 7. Verificación estructural sobre la IR

El verificador estructural recorre la IR, mantiene un sistema de scopes
encadenados que reflejan el alcance de las variables y los aliases en
cada nivel, y emite una lista de `VerificationError` cuando detecta
violaciones. Cada error está categorizado por una etiqueta canónica
(`kind`) que pertenece a una de las cuatro familias de chequeo.

### 7.1 Familia referencial (cap. 4)

Cubre seis kinds: `unknown_table`, `unknown_column`,
`unknown_qualifier`, `unknown_graph`, `unknown_vertex_label` y
`unknown_edge_label`. Cada uno se emite cuando la IR contiene una
referencia a un nombre que no está declarado en el esquema proyectado.
La resolución es case-insensitive, congruente con el comportamiento por
defecto de SQLite y de DuckDB sobre nombres de tabla y columna. El
sistema de scopes maneja correctamente los aliases declarados con `AS`
en el SELECT (visibles en `ORDER BY` y `HAVING`), los aliases de tablas
en el `FROM`, los aliases de subqueries y las variables de vértice
ligadas dentro de un MATCH (visibles solo dentro del `where` y el
`columns` del propio MATCH).

### 7.2 Familia de tipos (cap. 4)

Cubre dos kinds: `type_mismatch_aggregate` (cuando una agregación `AVG`
o `SUM` se aplica sobre una columna no numérica) y
`type_mismatch_arithmetic` (cuando una operación aritmética se aplica
sobre operandos no numéricos). La inferencia de tipo es por categoría
abstracta: `NUMERIC`, `TEXT`, `DATE`, `BOOLEAN`, `OTHER`, `ANY`,
`UNKNOWN`. La categoría se deriva de la cadena de tipo declarada en el
esquema; las columnas con tipos no resueltos se consideran `UNKNOWN` y
se aceptan permisivamente para evitar falsos positivos.

### 7.3 Familia de coherencia cruzada relacional↔grafo (cap. 4)

Cubre un kind: `vertex_label_without_table`. Se emite cuando el property
graph declara un label de vértice pero la tabla relacional asociada no
existe en el esquema relacional. La detección es importante para
escenarios donde el grafo y las tablas se declaran en archivos separados
y la coherencia entre ambos no está garantizada.

### 7.4 Familia de coherencia cruzada extendida (motivada por exp 07)

Cubre un kind: `path_step_incoherent`. Se emite cuando un step de un
`PathPattern` viola la declaración del edge en el property graph: por
ejemplo, intentar `(Company)-[lives_in]->(City)` cuando `lives_in` está
declarada como `Person → City`. La verificación contempla las tres
direcciones del `EdgePattern`: forward (`->`), backward (`<-`) y no
dirigida (`-`), aceptando cualquier orientación en el último caso.

Esta clase es una extensión natural del cap. 4 §4.5.4: el cap. 4 declara
tres clases originales (referencial, tipos, coherencia cruzada);
`path_step_incoherent` complementa la tercera familia con la dimensión
multihop específica del régimen híbrido. Se incorporó después de que el
experimento 07 exhibiera el modo de fallo correspondiente, con el
mensaje del verificador citando el step ofensivo y la declaración
esperada del edge.

### 7.5 Familia de restricciones operativas (motivada por exp 07b)

Cubre un kind: `missing_vertex_label`. Se emite cuando un
`VertexPattern` no tiene `label` declarado. En el estándar PGQ esto es
válido y significa "any vertex"; DuckPGQ rechaza la construcción con
"All patterns must bind to a label". Esta familia se mantiene
explícitamente separada de las clases del cap. 4 porque su origen es de
portabilidad operativa, no de diseño abstracto. La distinción es
relevante para el discurso del capítulo cuatro: las clases de las
familias 7.1 a 7.4 son contribución del enfoque y serían igualmente
relevantes para cualquier motor que implemente el estándar SQL/PGQ; la
clase 7.5 es ingeniería específica del motor objetivo y queda como
documentación del compromiso operativo.

### 7.6 Política de errores

El verificador acumula errores en una lista y los devuelve al final, en
lugar de cortar al primer fallo. Esta política está motivada por el uso
en el bucle de retroalimentación: un único reporte por consulta da al
agente que orquesta el pipeline información completa para decidir si
re-prompts, repara una porción de la IR o reformula la pregunta. Dentro
del verificador, ciertos chequeos se omiten cuando otro ya reportó un
problema sobre la misma posición (por ejemplo, no se intenta resolver
una columna calificada por un alias que ya se sabe inexistente), para
evitar inflar el reporte con errores derivados del mismo origen.

## 8. Compilación determinista a SQL/PGQ

El compilador `compile_query` recibe una `Query` y devuelve un string
SQL. La operación es por recorrido ascendente del árbol y no consulta
estado externo: dos llamadas a `compile_query` con argumentos
estructuralmente idénticos producen literalmente el mismo string. El
compilador es por construcción la inversa derecha del lifter sobre el
subset de SQL cubierto por la IR.

El compilador inserta paréntesis liberalmente alrededor de operadores
binarios y subconsultas para no depender de la precedencia implícita del
dialecto. La consecuencia es que el SQL emitido es verboso pero siempre
correcto, y el round-trip de lift y compile preserva la igualdad
estructural de la IR cuando se ignoran los `ParenExpr` cosméticos.

La compilación de bloques de grafo cumple las dos restricciones
operativas de DuckPGQ documentadas: emite una variable explícita en
cada arista (e.g., `[k:knows]` en lugar de `[:knows]`) y omite el
keyword `AS` antes del alias del bloque `GRAPH_TABLE` (DuckPGQ rechaza
`GRAPH_TABLE(...) AS g`; acepta `GRAPH_TABLE(...) g`). Estas
restricciones están aisladas en `_compile_from_graph_match` y
`_compile_edge_pattern`; cuando DuckPGQ las relaje en una versión
posterior, el cambio será local.

La compilación de operaciones de conjuntos sigue la convención de
envolver cada operando en paréntesis (`(left) UNION (right)`), lo que
evita ambigüedades en presencia de cláusulas adicionales en uno de los
operandos. El compilador no realiza optimización: la consulta producida
puede no ser la más eficiente, pero es válida por construcción respecto
al estándar y al esquema, y el motor de ejecución se hace cargo de la
optimización siguiendo el principio de separación de responsabilidades
declarado en el cap. 4.

## 9. Serialización JSON y contrato con el modelo

La IR tiene un contrato bidireccional con un formato JSON plano definido
en `core/ir/parse.py`. Cada nodo se representa como un objeto JSON con
un campo `type` cuyo valor es el nombre exacto de la dataclass; las
tuplas internas se representan como arrays JSON; los primitivos pasan
transparentes; las uniones discriminadas (`Query`, `Expression`,
`FromExpr`) resuelven por el campo `type` del objeto.

La función `to_dict` serializa una instancia de IR al formato JSON; la
función `parse_ir` realiza la operación inversa. Las dos son la
identidad mutua: `parse_ir(to_dict(q)) == q` para toda IR válida.
Cualquier desviación del formato (campo `type` ausente, nombre de tipo
desconocido, fields que no existen en la dataclass) eleva
`IRParseError` con un mensaje accionable.

El formato JSON se utiliza como `input_schema` de una tool de Anthropic
en el experimento cinco y siguientes. El JSON Schema correspondiente
está construido en `core/ir/json_schema.py` con definiciones encadenadas
por `$defs` y `$ref` para soportar la recursividad natural de la IR.
Está expresado en draft 2020-12, que es la versión que la API de
Anthropic exige para la validación de tool inputs.

El contrato JSON es lo que materializa la propiedad de cierre estructural
del cap. 4 §4.4 (P2). El modelo no puede emitir SQL libre porque la
única salida válida del tool es un payload IR; el SDK de Anthropic
valida el payload contra el JSON Schema antes de devolverlo;
`parse_ir` aplica una segunda validación contra las dataclasses; el
verificador estructural aplica la tercera contra el esquema proyectado.
Cuatro guardas en serie: por construcción, una alucinación de nombre no
puede llegar a la fase de compilación.

## 10. Limitaciones conocidas y trabajo futuro

La IR cubre el subconjunto de SQL ANSI que aparece en las cien
predicciones del experimento dos con noventa y nueve por ciento de
cobertura (round-trip estructural exacto). El uno por ciento restante
corresponde a una window function, feature deferida explícitamente.
Otras construcciones que la IR no representa todavía y que se
agregarían sin cambios estructurales cuando un experimento las exhiba:
expresiones de tipo `EXTRACT(field FROM expr)`, `INTERVAL`, funciones
con `OVER` (window), CTEs (`WITH ... AS (...)`) y CTEs recursivas. La
estrategia es permanecer minimalista hasta que la cobertura empírica
exija extenderla.

Sobre el bloque de grafo, la IR cubre los patrones `MATCH` con un solo
camino o múltiples caminos en paralelo (multipattern), las tres
direcciones de arista, los joins con tablas relacionales en el `FROM`
contenedor, y los filtros tanto internos al MATCH como externos. Lo que
no cubre todavía y que sería extensión natural: cuantificadores de
camino (`*`, `+`, `{n,m}`) que el estándar SQL/PGQ define para caminos
de longitud variable, y las construcciones `ALL SHORTEST PATH` y `ANY
PATH`.

El verificador estructural, fuera de las cinco clases de error
implementadas, podría extenderse con dos chequeos adicionales que el
cap. 4 menciona pero que la implementación actual no realiza: la
verificación de unicidad de columnas en cláusulas de agregación (toda
columna en el SELECT debe estar en el GROUP BY o ser una agregación) y
la verificación de cardinalidades esperadas (por ejemplo, una expresión
en posición escalar no puede ser un `Subquery` que retorne más de una
fila). Ambos requieren más lógica de scope que la actualmente
implementada y se mencionan como trabajo futuro.

El régimen de verificación dinámica del cap. 4 §4.5.4 no está todavía
implementado. La idea es que cuando una IR pasa la verificación estática
y se ejecuta sobre el motor, los errores de ejecución (o salidas
atípicas como cardinalidad cero o presencia inesperada de nulos) se
traduzcan a un descriptor categórico y se reinyecten al modelo como
contexto adicional para una nueva iteración de generación. La
implementación de este bucle requiere componentes que la IR no
directamente expone (clasificador de errores del motor, decisor de
reintentos, mediador de contexto) y queda como la próxima rebanada
sustantiva de trabajo.
