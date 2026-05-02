"""
JSON Schema de la IR-SQL/PGQ para tool use con Anthropic.

El schema define el contrato que el modelo debe respetar al emitir una IR.
Su valor en el flujo de la rebanada 5 es triple:

1. Es el ``input_schema`` del tool ``submit_query`` que el modelo invoca, lo
   que fuerza una salida estructurada en lugar de prosa libre.
2. La SDK de Anthropic valida el payload contra el schema antes de
   devolverlo, dando una primera red de seguridad antes del parser propio.
3. Documenta de forma máquinable el formato; cualquier consumidor (humano o
   programático) puede usarlo como fuente de verdad.

El schema NO reemplaza a ``core.ir.parse.parse_ir``; este último hace la
validación estructural definitiva contra las dataclasses. Mantenerlos
separados evita acoplar el contrato externo (JSON Schema, hereda evolución
del modelo) al contrato interno (dataclasses, hereda evolución del compiler
y verifier).

El schema cubre el bloque relacional completo y los nodos de grafo. Está
construido con ``$defs`` y ``$ref`` para evitar duplicación dada la
recursividad natural de la IR (expresiones que contienen expresiones,
queries que contienen subqueries, etc.).
"""

from __future__ import annotations

from typing import Any


def _ref(name: str) -> dict[str, Any]:
    return {"$ref": f"#/$defs/{name}"}


def build_ir_tool_schema() -> dict[str, Any]:
    """Devuelve el JSON Schema completo de la IR como input_schema de tool.

    El nivel raíz delega a ``Query`` para que el modelo sólo pueda emitir un
    objeto que sea ``RelationalQuery`` o ``SetOperation``. Las definiciones
    auxiliares viven en ``$defs``.
    """

    defs: dict[str, Any] = {}

    # ----- Referencias a esquema --------------------------------------------------

    defs["TableRef"] = {
        "type": "object",
        "properties": {
            "type": {"const": "TableRef"},
            "name": {"type": "string", "description": "Nombre de la tabla en el esquema."},
            "alias": {"type": ["string", "null"], "default": None},
        },
        "required": ["type", "name"],
    }

    defs["ColumnRef"] = {
        "type": "object",
        "properties": {
            "type": {"const": "ColumnRef"},
            "name": {"type": "string"},
            "qualifier": {
                "type": ["string", "null"],
                "description": (
                    "Alias de tabla o variable de vértice que califica la columna; "
                    "null para columna desnuda."
                ),
            },
        },
        "required": ["type", "name"],
    }

    # ----- Expresiones -----------------------------------------------------------

    defs["Literal"] = {
        "type": "object",
        "properties": {
            "type": {"const": "Literal"},
            "value": {
                "description": "Valor: string, número, booleano o null.",
            },
            "raw": {"type": ["string", "null"]},
        },
        "required": ["type", "value"],
    }

    defs["ColumnExpr"] = {
        "type": "object",
        "properties": {
            "type": {"const": "ColumnExpr"},
            "ref": _ref("ColumnRef"),
        },
        "required": ["type", "ref"],
    }

    defs["Star"] = {
        "type": "object",
        "properties": {
            "type": {"const": "Star"},
            "qualifier": {"type": ["string", "null"], "default": None},
        },
        "required": ["type"],
    }

    defs["BinaryOp"] = {
        "type": "object",
        "properties": {
            "type": {"const": "BinaryOp"},
            "op": {
                "type": "string",
                "description": (
                    "Uno de: =, <>, <, <=, >, >=, +, -, *, /, %, AND, OR."
                ),
            },
            "left": _ref("Expression"),
            "right": _ref("Expression"),
        },
        "required": ["type", "op", "left", "right"],
    }

    defs["UnaryOp"] = {
        "type": "object",
        "properties": {
            "type": {"const": "UnaryOp"},
            "op": {"type": "string", "description": "NOT, -."},
            "operand": _ref("Expression"),
        },
        "required": ["type", "op", "operand"],
    }

    defs["FunctionCall"] = {
        "type": "object",
        "properties": {
            "type": {"const": "FunctionCall"},
            "name": {"type": "string"},
            "args": {"type": "array", "items": _ref("Expression"), "default": []},
        },
        "required": ["type", "name"],
    }

    defs["Aggregate"] = {
        "type": "object",
        "properties": {
            "type": {"const": "Aggregate"},
            "name": {
                "type": "string",
                "description": "COUNT, SUM, AVG, MIN, MAX (en mayúsculas).",
            },
            "args": {"type": "array", "items": _ref("Expression"), "default": []},
            "distinct": {"type": "boolean", "default": False},
        },
        "required": ["type", "name"],
    }

    defs["CaseExpr"] = {
        "type": "object",
        "properties": {
            "type": {"const": "CaseExpr"},
            "branches": {
                "type": "array",
                "items": {
                    "type": "array",
                    "minItems": 2,
                    "maxItems": 2,
                    "prefixItems": [_ref("Expression"), _ref("Expression")],
                    "description": "[condición, valor]",
                },
            },
            "else_": {"oneOf": [_ref("Expression"), {"type": "null"}]},
        },
        "required": ["type", "branches"],
    }

    defs["CastExpr"] = {
        "type": "object",
        "properties": {
            "type": {"const": "CastExpr"},
            "expr": _ref("Expression"),
            "type_name": {"type": "string"},
        },
        "required": ["type", "expr", "type_name"],
    }

    defs["LikeExpr"] = {
        "type": "object",
        "properties": {
            "type": {"const": "LikeExpr"},
            "left": _ref("Expression"),
            "pattern": _ref("Expression"),
            "negate": {"type": "boolean", "default": False},
        },
        "required": ["type", "left", "pattern"],
    }

    defs["InExpr"] = {
        "type": "object",
        "properties": {
            "type": {"const": "InExpr"},
            "left": _ref("Expression"),
            "rhs": {
                "oneOf": [
                    {"type": "array", "items": _ref("Expression")},
                    _ref("Subquery"),
                ],
                "description": (
                    "Tupla de expresiones literales o una subconsulta envuelta en Subquery."
                ),
            },
            "negate": {"type": "boolean", "default": False},
        },
        "required": ["type", "left", "rhs"],
    }

    defs["IsNullExpr"] = {
        "type": "object",
        "properties": {
            "type": {"const": "IsNullExpr"},
            "operand": _ref("Expression"),
            "negate": {"type": "boolean", "default": False},
        },
        "required": ["type", "operand"],
    }

    defs["BetweenExpr"] = {
        "type": "object",
        "properties": {
            "type": {"const": "BetweenExpr"},
            "operand": _ref("Expression"),
            "low": _ref("Expression"),
            "high": _ref("Expression"),
            "negate": {"type": "boolean", "default": False},
        },
        "required": ["type", "operand", "low", "high"],
    }

    defs["ExistsExpr"] = {
        "type": "object",
        "properties": {
            "type": {"const": "ExistsExpr"},
            "query": _ref("Query"),
            "negate": {"type": "boolean", "default": False},
        },
        "required": ["type", "query"],
    }

    defs["Subquery"] = {
        "type": "object",
        "properties": {
            "type": {"const": "Subquery"},
            "query": _ref("Query"),
        },
        "required": ["type", "query"],
    }

    defs["ParenExpr"] = {
        "type": "object",
        "properties": {
            "type": {"const": "ParenExpr"},
            "inner": _ref("Expression"),
        },
        "required": ["type", "inner"],
    }

    defs["Expression"] = {
        "oneOf": [
            _ref("Literal"),
            _ref("ColumnExpr"),
            _ref("Star"),
            _ref("BinaryOp"),
            _ref("UnaryOp"),
            _ref("FunctionCall"),
            _ref("Aggregate"),
            _ref("CaseExpr"),
            _ref("CastExpr"),
            _ref("LikeExpr"),
            _ref("InExpr"),
            _ref("IsNullExpr"),
            _ref("BetweenExpr"),
            _ref("ExistsExpr"),
            _ref("Subquery"),
            _ref("ParenExpr"),
        ]
    }

    # ----- Cláusulas -------------------------------------------------------------

    defs["SelectItem"] = {
        "type": "object",
        "properties": {
            "type": {"const": "SelectItem"},
            "expr": _ref("Expression"),
            "alias": {"type": ["string", "null"], "default": None},
        },
        "required": ["type", "expr"],
    }

    defs["OrderItem"] = {
        "type": "object",
        "properties": {
            "type": {"const": "OrderItem"},
            "expr": _ref("Expression"),
            "direction": {"type": "string", "enum": ["ASC", "DESC"], "default": "ASC"},
            "nulls": {
                "oneOf": [
                    {"type": "string", "enum": ["FIRST", "LAST"]},
                    {"type": "null"},
                ]
            },
        },
        "required": ["type", "expr"],
    }

    # ----- FROM y JOIN -----------------------------------------------------------

    defs["FromTable"] = {
        "type": "object",
        "properties": {
            "type": {"const": "FromTable"},
            "table": _ref("TableRef"),
        },
        "required": ["type", "table"],
    }

    defs["FromSubquery"] = {
        "type": "object",
        "properties": {
            "type": {"const": "FromSubquery"},
            "query": _ref("Query"),
            "alias": {"type": "string"},
        },
        "required": ["type", "query", "alias"],
    }

    defs["Join"] = {
        "type": "object",
        "properties": {
            "type": {"const": "Join"},
            "left": _ref("FromExpr"),
            "right": _ref("FromExpr"),
            "kind": {
                "type": "string",
                "description": "INNER, LEFT, RIGHT, FULL, CROSS, NATURAL.",
            },
            "on": {"oneOf": [_ref("Expression"), {"type": "null"}]},
            "using": {
                "oneOf": [
                    {"type": "array", "items": {"type": "string"}},
                    {"type": "null"},
                ]
            },
        },
        "required": ["type", "left", "right", "kind"],
    }

    defs["FromGraphMatch"] = {
        "type": "object",
        "properties": {
            "type": {"const": "FromGraphMatch"},
            "match": _ref("MatchPattern"),
            "alias": {"type": ["string", "null"], "default": None},
        },
        "required": ["type", "match"],
    }

    defs["FromExpr"] = {
        "oneOf": [
            _ref("FromTable"),
            _ref("FromSubquery"),
            _ref("Join"),
            _ref("FromGraphMatch"),
        ]
    }

    # ----- Bloques de grafo ------------------------------------------------------

    defs["VertexPattern"] = {
        "type": "object",
        "properties": {
            "type": {"const": "VertexPattern"},
            "var": {"type": "string"},
            "label": {"type": ["string", "null"], "default": None},
        },
        "required": ["type", "var"],
    }

    defs["EdgePattern"] = {
        "type": "object",
        "properties": {
            "type": {"const": "EdgePattern"},
            "var": {
                "type": "string",
                "description": (
                    "Variable obligatoria por restricción del motor DuckPGQ, "
                    "incluso si no se usa en COLUMNS."
                ),
            },
            "label": {"type": ["string", "null"], "default": None},
            "direction": {"type": "string", "enum": ["->", "<-", "-"], "default": "->"},
        },
        "required": ["type", "var"],
    }

    defs["PathPattern"] = {
        "type": "object",
        "properties": {
            "type": {"const": "PathPattern"},
            "head": _ref("VertexPattern"),
            "steps": {
                "type": "array",
                "items": {
                    "type": "array",
                    "minItems": 2,
                    "maxItems": 2,
                    "prefixItems": [_ref("EdgePattern"), _ref("VertexPattern")],
                    "description": "[arista, vértice destino]",
                },
                "default": [],
            },
        },
        "required": ["type", "head"],
    }

    defs["MatchPattern"] = {
        "type": "object",
        "properties": {
            "type": {"const": "MatchPattern"},
            "graph": {"type": "string"},
            "patterns": {"type": "array", "items": _ref("PathPattern")},
            "where": {"oneOf": [_ref("Expression"), {"type": "null"}]},
            "columns": {"type": "array", "items": _ref("SelectItem"), "default": []},
        },
        "required": ["type", "graph", "patterns"],
    }

    defs["GraphQuery"] = {
        "type": "object",
        "properties": {
            "type": {"const": "GraphQuery"},
            "match": _ref("MatchPattern"),
        },
        "required": ["type", "match"],
    }

    # ----- Top-level Query -------------------------------------------------------

    defs["RelationalQuery"] = {
        "type": "object",
        "properties": {
            "type": {"const": "RelationalQuery"},
            "select": {"type": "array", "items": _ref("SelectItem"), "minItems": 1},
            "from_": {"oneOf": [_ref("FromExpr"), {"type": "null"}]},
            "where": {"oneOf": [_ref("Expression"), {"type": "null"}]},
            "group_by": {"type": "array", "items": _ref("Expression"), "default": []},
            "having": {"oneOf": [_ref("Expression"), {"type": "null"}]},
            "order_by": {"type": "array", "items": _ref("OrderItem"), "default": []},
            "limit": {"type": ["integer", "null"]},
            "offset": {"type": ["integer", "null"]},
            "distinct": {"type": "boolean", "default": False},
        },
        "required": ["type", "select"],
    }

    defs["SetOperation"] = {
        "type": "object",
        "properties": {
            "type": {"const": "SetOperation"},
            "op": {
                "type": "string",
                "description": (
                    "UNION, UNION ALL, INTERSECT, INTERSECT ALL, EXCEPT, EXCEPT ALL."
                ),
            },
            "left": _ref("Query"),
            "right": _ref("Query"),
        },
        "required": ["type", "op", "left", "right"],
    }

    defs["Query"] = {
        "oneOf": [_ref("RelationalQuery"), _ref("SetOperation")]
    }

    # ----- Schema raíz -----------------------------------------------------------

    return {
        "type": "object",
        "properties": {
            "query": _ref("Query"),
        },
        "required": ["query"],
        "$defs": defs,
    }


IR_TOOL_INPUT_SCHEMA: dict[str, Any] = build_ir_tool_schema()
