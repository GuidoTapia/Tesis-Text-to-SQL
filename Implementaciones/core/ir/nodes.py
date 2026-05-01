"""
Definición de los nodos de la IR-SQL/PGQ (capítulo 4 §4.5.2 de la tesis).

La IR es la única representación válida que las etapas de generación, verificación
y compilación intercambian. Es un árbol tipado e inmutable construido con
``@dataclass(frozen=True)`` para garantizar igualdad estructural, hashability y
ausencia de mutación accidental.

Esta rebanada cubre el bloque relacional. Los nodos del bloque de grafo
(``VertexPattern``, ``EdgePattern``, ``MatchPattern``, ``GraphQuery``) están
declarados como placeholders para fijar el vocabulario, pero su lifter y
compiler quedan para la rebanada 2. Lo mismo aplica a ``HybridComposition``
(rebanada 3).
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional, Union


# ---------------------------------------------------------------------------
# Referencias a esquema
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class TableRef:
    """Referencia a una tabla en una cláusula FROM o JOIN."""

    name: str
    alias: Optional[str] = None


@dataclass(frozen=True)
class ColumnRef:
    """Referencia a una columna. ``qualifier`` puede ser un nombre de tabla
    o un alias declarado en la misma consulta."""

    name: str
    qualifier: Optional[str] = None


# ---------------------------------------------------------------------------
# Expresiones
# ---------------------------------------------------------------------------


class Expression:
    """Marcador base para toda expresión que produce un valor."""


@dataclass(frozen=True)
class Literal(Expression):
    """Constante literal: número, cadena, booleano o NULL."""

    value: Union[str, int, float, bool, None]
    # ``raw`` preserva la forma como apareció en el SQL original (con comillas o
    # sin ellas) para que el compilador la pueda reproducir fielmente.
    raw: Optional[str] = None


@dataclass(frozen=True)
class ColumnExpr(Expression):
    """Una ColumnRef usada como expresión."""

    ref: ColumnRef


@dataclass(frozen=True)
class Star(Expression):
    """El símbolo ``*`` en ``SELECT *`` o ``COUNT(*)``. ``qualifier`` vale
    cuando se escribe ``t.*``."""

    qualifier: Optional[str] = None


@dataclass(frozen=True)
class BinaryOp(Expression):
    """Operador binario aritmético, de comparación o lógico."""

    op: str
    left: Expression
    right: Expression


@dataclass(frozen=True)
class UnaryOp(Expression):
    op: str
    operand: Expression


@dataclass(frozen=True)
class FunctionCall(Expression):
    """Función escalar (lower, coalesce, length, etc.)."""

    name: str
    args: tuple[Expression, ...] = ()


@dataclass(frozen=True)
class Aggregate(Expression):
    """Función de agregación con marcador opcional ``DISTINCT``."""

    name: str
    args: tuple[Expression, ...] = ()
    distinct: bool = False


@dataclass(frozen=True)
class CaseExpr(Expression):
    branches: tuple[tuple[Expression, Expression], ...]
    else_: Optional[Expression] = None


@dataclass(frozen=True)
class CastExpr(Expression):
    expr: Expression
    type_name: str


@dataclass(frozen=True)
class LikeExpr(Expression):
    left: Expression
    pattern: Expression
    negate: bool = False


@dataclass(frozen=True)
class InExpr(Expression):
    """``x IN (...)``. ``rhs`` puede ser una tupla de expresiones o una
    subconsulta."""

    left: Expression
    rhs: Union[tuple[Expression, ...], "Subquery"]
    negate: bool = False


@dataclass(frozen=True)
class IsNullExpr(Expression):
    operand: Expression
    negate: bool = False


@dataclass(frozen=True)
class BetweenExpr(Expression):
    operand: Expression
    low: Expression
    high: Expression
    negate: bool = False


@dataclass(frozen=True)
class ExistsExpr(Expression):
    query: "Query"
    negate: bool = False


@dataclass(frozen=True)
class Subquery(Expression):
    query: "Query"


@dataclass(frozen=True)
class ParenExpr(Expression):
    """Agrupación explícita. El lifter no la introduce salvo cuando es
    necesaria; el compilador siempre puede agregar paréntesis adicionales por
    seguridad."""

    inner: Expression


# ---------------------------------------------------------------------------
# Item de SELECT
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class SelectItem:
    expr: Expression
    alias: Optional[str] = None


# ---------------------------------------------------------------------------
# Cláusula FROM y JOIN
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class FromTable:
    table: TableRef


@dataclass(frozen=True)
class FromSubquery:
    query: "Query"
    alias: str


@dataclass(frozen=True)
class Join:
    left: "FromExpr"
    right: "FromExpr"
    kind: str  # INNER, LEFT, RIGHT, FULL, CROSS, NATURAL
    on: Optional[Expression] = None
    using: Optional[tuple[str, ...]] = None


FromExpr = Union[FromTable, FromSubquery, Join]


# ---------------------------------------------------------------------------
# ORDER BY
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class OrderItem:
    expr: Expression
    direction: str = "ASC"
    nulls: Optional[str] = None  # FIRST, LAST


# ---------------------------------------------------------------------------
# Consultas relacionales
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class RelationalQuery:
    """Consulta relacional canónica.

    Fields siguen el orden lógico de evaluación de SQL para que el compiler
    pueda emitirlos de manera lineal.
    """

    select: tuple[SelectItem, ...]
    from_: Optional[FromExpr] = None
    where: Optional[Expression] = None
    group_by: tuple[Expression, ...] = ()
    having: Optional[Expression] = None
    order_by: tuple[OrderItem, ...] = ()
    limit: Optional[int] = None
    offset: Optional[int] = None
    distinct: bool = False


@dataclass(frozen=True)
class SetOperation:
    """UNION, INTERSECT, EXCEPT (con o sin ALL)."""

    op: str  # UNION, UNION ALL, INTERSECT, INTERSECT ALL, EXCEPT, EXCEPT ALL
    left: "Query"
    right: "Query"


Query = Union[RelationalQuery, SetOperation]


# ---------------------------------------------------------------------------
# Bloques de grafo (placeholders para rebanadas 2 y 3)
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class VertexPattern:
    """Patrón ``(var:Label)`` dentro de un MATCH."""

    var: str
    label: Optional[str] = None


@dataclass(frozen=True)
class EdgePattern:
    """Patrón ``[var:Label]`` dentro de un MATCH. DuckPGQ exige ``var`` aunque
    no se use."""

    var: str
    label: Optional[str] = None
    direction: str = "->"  # "->", "<-", "-"


@dataclass(frozen=True)
class PathPattern:
    """Patrón de camino: vértice, secuencia alternada de (arista, vértice)."""

    head: VertexPattern
    steps: tuple[tuple[EdgePattern, VertexPattern], ...] = ()


@dataclass(frozen=True)
class MatchPattern:
    """Cláusula ``GRAPH_TABLE(graph MATCH p1, p2, ... [WHERE ...] COLUMNS (...))``."""

    graph: str
    patterns: tuple[PathPattern, ...]
    where: Optional[Expression] = None
    columns: tuple[SelectItem, ...] = ()


@dataclass(frozen=True)
class GraphQuery:
    """Consulta de grafo independiente. Emite directamente
    ``FROM GRAPH_TABLE(...)`` como cuerpo principal."""

    match: MatchPattern


# Composición híbrida (rebanada 3): el resultado de un MatchPattern aparece
# como tabla derivada en una FromExpr relacional. Sólo declarado.


@dataclass(frozen=True)
class FromGraphMatch:
    match: MatchPattern
    alias: str


# Cuando rebanada 3 esté implementada, FromExpr se ampliará a
# ``Union[FromTable, FromSubquery, Join, FromGraphMatch]``.
