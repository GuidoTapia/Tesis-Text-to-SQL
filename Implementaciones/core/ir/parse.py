"""
Parser y serializador bidireccional entre la IR-SQL/PGQ y un formato JSON
plano.

El formato es el contrato entre el LLM y el resto del pipeline (rebanada 5):
el modelo emite un objeto JSON; ``parse_ir`` lo convierte a una instancia de
IR; el verificador estructural opera sobre esa IR; y el compilador la
materializa a SQL/PGQ. Ningún SQL fluye hacia o desde el LLM en este flujo.

Convenciones del formato:

- Cada nodo es un objeto JSON con un campo ``type`` cuyo valor es el nombre
  exacto de la dataclass (e.g. ``"ColumnExpr"``, ``"RelationalQuery"``).
- Las colecciones de la IR son tuplas inmutables; en JSON viajan como arrays
  y el parser las re-empaqueta como tuplas.
- ``None`` ↔ ``null``. ``True/False`` ↔ ``true/false``. Strings y números
  pasan transparentes.
- Las uniones discriminadas (``Query``, ``Expression``, ``FromExpr``)
  resuelven por el campo ``type``.
- El serializador es la función inversa: ``parse_ir(to_dict(x)) == x``.

Errores de formato (campo ``type`` ausente, nombre de tipo desconocido,
fields inválidos para la dataclass) elevan ``IRParseError`` con un mensaje
descriptivo. El parser no intenta corregir nada: prefiere fallar ruidoso a
fabricar una IR aproximada.
"""

from __future__ import annotations

import dataclasses
from typing import Any

from . import nodes as ir


class IRParseError(ValueError):
    """Error de parseo: el JSON recibido no se puede mapear a la IR."""


# Mapa nombre → clase. Lo construimos explícitamente para evitar incluir
# accidentalmente clases auxiliares o helpers.
_NODE_TYPES: dict[str, type] = {
    cls.__name__: cls
    for cls in (
        # Referencias a esquema
        ir.TableRef,
        ir.ColumnRef,
        # Expresiones
        ir.Literal,
        ir.ColumnExpr,
        ir.Star,
        ir.BinaryOp,
        ir.UnaryOp,
        ir.FunctionCall,
        ir.Aggregate,
        ir.CaseExpr,
        ir.CastExpr,
        ir.LikeExpr,
        ir.InExpr,
        ir.IsNullExpr,
        ir.BetweenExpr,
        ir.ExistsExpr,
        ir.Subquery,
        ir.ParenExpr,
        # Cláusulas
        ir.SelectItem,
        ir.OrderItem,
        # FROM y composición
        ir.FromTable,
        ir.FromSubquery,
        ir.Join,
        ir.FromGraphMatch,
        # Top-level
        ir.RelationalQuery,
        ir.SetOperation,
        # Bloques de grafo
        ir.VertexPattern,
        ir.EdgePattern,
        ir.PathPattern,
        ir.MatchPattern,
        ir.GraphQuery,
    )
}


# Conjunto de nombres de tipo que el parser reconoce. Útil para mensajes de
# error con sugerencias de tipos válidos.
KNOWN_TYPES: frozenset[str] = frozenset(_NODE_TYPES)


# ---------------------------------------------------------------------------
# Parser JSON → IR
# ---------------------------------------------------------------------------


def parse_ir(data: Any) -> Any:
    """Parsea un valor JSON-decodificado y devuelve la IR equivalente.

    Recursivo. Las listas se convierten a tuplas. Los dicts con campo
    ``type`` se convierten a la dataclass correspondiente. Los primitivos
    (None, bool, int, float, str) pasan sin cambio.

    Eleva ``IRParseError`` si el formato es inválido.
    """
    if data is None or isinstance(data, (bool, int, float, str)):
        return data
    if isinstance(data, list):
        return tuple(parse_ir(x) for x in data)
    if not isinstance(data, dict):
        raise IRParseError(f"valor JSON no soportado: {type(data).__name__}")

    type_name = data.get("type")
    if type_name is None:
        keys_preview = list(data.keys())[:5]
        raise IRParseError(
            f"objeto JSON sin campo 'type'; claves presentes: {keys_preview}"
        )
    cls = _NODE_TYPES.get(type_name)
    if cls is None:
        raise IRParseError(
            f"tipo de nodo desconocido: {type_name!r}. "
            f"Tipos válidos disponibles en core.ir.parse.KNOWN_TYPES."
        )

    raw_fields = {k: v for k, v in data.items() if k != "type"}
    parsed_fields = {k: parse_ir(v) for k, v in raw_fields.items()}
    try:
        return cls(**parsed_fields)
    except TypeError as exc:
        # Mensaje útil: explicita qué fields esperaba la dataclass
        expected = [f.name for f in dataclasses.fields(cls)]
        got = list(parsed_fields.keys())
        raise IRParseError(
            f"campos inválidos para {type_name}: {exc}. "
            f"Esperaba: {expected}. Recibió: {got}."
        ) from exc


# ---------------------------------------------------------------------------
# Serializador IR → JSON-compatible dict
# ---------------------------------------------------------------------------


def to_dict(node: Any) -> Any:
    """Serializa un nodo IR (o un valor primitivo) a un objeto JSON-compatible.

    Recursivo. Tuplas se convierten a listas. Dataclasses agregan el campo
    ``type`` con el nombre exacto de la clase. Los primitivos pasan sin cambio.

    Es la inversa de ``parse_ir``: la composición de ambos preserva la IR
    original.
    """
    if node is None or isinstance(node, (bool, int, float, str)):
        return node
    if isinstance(node, tuple):
        return [to_dict(x) for x in node]
    if isinstance(node, list):
        return [to_dict(x) for x in node]
    if dataclasses.is_dataclass(node):
        out: dict[str, Any] = {"type": type(node).__name__}
        for f in dataclasses.fields(node):
            out[f.name] = to_dict(getattr(node, f.name))
        return out
    raise TypeError(
        f"to_dict: no se puede serializar valor de tipo {type(node).__name__}"
    )
