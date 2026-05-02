# Formato JSON de la IR-SQL/PGQ

Documento de referencia del formato JSON que la IR usa como contrato de
serialización. El parser bidireccional vive en `core/ir/parse.py`; las
dataclasses canónicas en `core/ir/nodes.py`.

Este documento está pensado tanto para humanos que diseñan prompts del LLM
como para incluir directamente como contexto en los system prompts.

## Convenciones

- **Cada nodo** es un objeto JSON con un campo `type` cuyo valor es el
  nombre exacto de la dataclass.
- **Las colecciones** viajan como arrays JSON. El parser las re-empaqueta
  como tuplas inmutables al construir la IR.
- **Los primitivos** pasan transparentes: `null`, `true`/`false`, números,
  strings.
- **Las uniones** (`Query`, `Expression`, `FromExpr`) resuelven por el campo
  `type`.
- **No hay** ningún campo opcional que el LLM pueda omitir: aunque la
  dataclass tenga default, conviene pasarlo explícito (e.g. `"alias": null`)
  para evitar ambigüedad.

## Catálogo de tipos

### Top-level

```
RelationalQuery     SetOperation
```

### Expresiones

```
Literal             ColumnExpr          Star
BinaryOp            UnaryOp             FunctionCall
Aggregate           CaseExpr            CastExpr
LikeExpr            InExpr              IsNullExpr
BetweenExpr         ExistsExpr          Subquery
ParenExpr
```

### Cláusulas

```
SelectItem          OrderItem
```

### FROM y composición

```
FromTable           FromSubquery        Join
FromGraphMatch
```

### Bloques de grafo

```
VertexPattern       EdgePattern         PathPattern
MatchPattern        GraphQuery
```

### Referencias a esquema

```
TableRef            ColumnRef
```

## Ejemplos canónicos

### 1. Consulta relacional simple

```sql
SELECT count(*) FROM singer
```

```json
{
  "type": "RelationalQuery",
  "select": [
    {"type": "SelectItem",
     "expr": {"type": "Aggregate", "name": "COUNT",
              "args": [{"type": "Star", "qualifier": null}],
              "distinct": false},
     "alias": null}
  ],
  "from_": {"type": "FromTable",
            "table": {"type": "TableRef", "name": "singer", "alias": null}},
  "where": null, "group_by": [], "having": null,
  "order_by": [], "limit": null, "offset": null, "distinct": false
}
```

### 2. Filtro con WHERE y orden

```sql
SELECT name FROM singer WHERE age > 30 ORDER BY age DESC LIMIT 5
```

```json
{
  "type": "RelationalQuery",
  "select": [
    {"type": "SelectItem",
     "expr": {"type": "ColumnExpr",
              "ref": {"type": "ColumnRef", "name": "name", "qualifier": null}},
     "alias": null}
  ],
  "from_": {"type": "FromTable",
            "table": {"type": "TableRef", "name": "singer", "alias": null}},
  "where": {
    "type": "BinaryOp", "op": ">",
    "left": {"type": "ColumnExpr",
             "ref": {"type": "ColumnRef", "name": "age", "qualifier": null}},
    "right": {"type": "Literal", "value": 30, "raw": "30"}
  },
  "group_by": [], "having": null,
  "order_by": [
    {"type": "OrderItem",
     "expr": {"type": "ColumnExpr",
              "ref": {"type": "ColumnRef", "name": "age", "qualifier": null}},
     "direction": "DESC", "nulls": null}
  ],
  "limit": 5, "offset": null, "distinct": false
}
```

### 3. Consulta de grafo pura

```sql
SELECT * FROM GRAPH_TABLE (test_graph
  MATCH (a:Person)-[k:knows]->(b:Person)
  COLUMNS (a.name AS src, b.name AS dst)) g
```

```json
{
  "type": "RelationalQuery",
  "select": [{"type": "SelectItem",
              "expr": {"type": "Star", "qualifier": null}, "alias": null}],
  "from_": {
    "type": "FromGraphMatch",
    "alias": "g",
    "match": {
      "type": "MatchPattern",
      "graph": "test_graph",
      "patterns": [{
        "type": "PathPattern",
        "head": {"type": "VertexPattern", "var": "a", "label": "Person"},
        "steps": [[
          {"type": "EdgePattern", "var": "k", "label": "knows", "direction": "->"},
          {"type": "VertexPattern", "var": "b", "label": "Person"}
        ]]
      }],
      "where": null,
      "columns": [
        {"type": "SelectItem",
         "expr": {"type": "ColumnExpr",
                  "ref": {"type": "ColumnRef", "name": "name", "qualifier": "a"}},
         "alias": "src"},
        {"type": "SelectItem",
         "expr": {"type": "ColumnExpr",
                  "ref": {"type": "ColumnRef", "name": "name", "qualifier": "b"}},
         "alias": "dst"}
      ]
    }
  },
  "where": null, "group_by": [], "having": null,
  "order_by": [], "limit": null, "offset": null, "distinct": false
}
```

### 4. Composición híbrida

```sql
SELECT g.src, p.age FROM GRAPH_TABLE (test_graph
  MATCH (a:Person)-[k:knows]->(b:Person)
  COLUMNS (a.name AS src, a.id AS src_id)) g
JOIN Person AS p ON g.src_id = p.id
WHERE p.age > 25
```

El `from_` es un `Join`; su `left` es un `FromGraphMatch` y su `right` es
una `FromTable`. El WHERE externo accede a `p.age`, columna que solo existe
del lado relacional.

## Reglas operativas para el LLM

1. **Sólo emitir JSON.** No texto explicativo, no markdown fences. La
   respuesta debe ser un único objeto JSON válido.
2. **Usar `null` explícito** para fields opcionales. Es preferible a
   omitirlos.
3. **Las colecciones siempre son arrays**, incluso cuando tienen un solo
   elemento. Por ejemplo, `select: [{"type": "SelectItem", ...}]`.
4. **Variables de aristas son obligatorias** en bloques de grafo (restricción
   del motor DuckPGQ): el `EdgePattern` siempre debe tener `var` no vacío.
5. **Comprometerse** con un nombre exacto del esquema en cada `ColumnRef` y
   `TableRef`. Si la respuesta requiere información que no está en el
   esquema, devolver un `RelationalQuery` con un `Literal` explicativo en el
   `select` no es válido — el sistema espera SQL semánticamente coherente.

## Errores comunes que el parser rechaza

- Falta el campo `type` en algún objeto.
- El valor de `type` no está en el catálogo.
- Una dataclass recibe fields que no declara (typo en el nombre del campo).
- Una colección esperada como array llega como string u objeto.

Cualquiera de estos eleva `IRParseError` con un mensaje que indica el
campo, el tipo y los nombres válidos esperados.
