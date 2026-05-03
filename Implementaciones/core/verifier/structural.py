"""
Verificador estructural sobre IR-SQL/PGQ (rebanada 4 de la implementación,
cap. 4 §4.5.4 de la tesis).

A diferencia del verificador MVP en ``static.py``, que opera sobre el AST de
sqlglot y solo chequea existencia de nombres, este verificador trabaja sobre
la IR tipada y materializa las tres clases de chequeo del cap. 4:

1. **Consistencia referencial.** Toda referencia a tabla, columna, grafo,
   etiqueta de vértice o etiqueta de arista debe corresponder a un elemento
   de Σ_proj.
2. **Consistencia de tipos.** Operadores aplicados a operandos compatibles.
   Versión inicial: ``AVG``/``SUM`` solo sobre columnas numéricas; aritmética
   binaria solo entre tipos numéricos.
3. **Coherencia cruzada relacional↔grafo.** En bloques de grafo, cada label
   de vértice referenciado debe tener una tabla relacional asociada (declarada
   por el property graph) que exista efectivamente en el esquema relacional.

El verificador acumula errores y devuelve la lista completa al final, en lugar
de cortar al primer fallo. Eso permite reportar todos los problemas de una
consulta de una vez.

Limitaciones conocidas (a resolver en iteraciones posteriores):
- Las subconsultas correlacionadas resuelven referencias contra el scope
  inmediatamente externo pero no propagan más allá de un nivel.
- La resolución de columnas no qualifier es por unión: no detecta ambigüedad
  cuando dos tablas distintas en scope tienen una columna con el mismo nombre.
- ``HAVING`` puede referirse a aliases del SELECT en muchos dialectos; el
  verificador acepta esa visibilidad sin chequear si la sintaxis estándar
  estricta lo permite.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional

from core.ir import nodes as ir
from core.ir.schema import (
    ColumnSchema,
    ProjectedSchema,
    PropertyGraphSchema,
    TableSchema,
)
from core.verifier.errors import VerificationError


# ---------------------------------------------------------------------------
# Tipos canónicos
# ---------------------------------------------------------------------------


# Mapeamos tipos de columna textuales a categorías abstractas que el verificador
# usa. Spider declara ``text``, ``number``, ``time``, ``boolean``, ``others``;
# DuckDB declara INTEGER, BIGINT, VARCHAR, REAL, DATE, TIMESTAMP, BOOLEAN, etc.
_NUMERIC_KEYWORDS = (
    "INT",
    "REAL",
    "FLOAT",
    "DOUBLE",
    "DECIMAL",
    "NUMERIC",
    "NUMBER",
)
_TEXT_KEYWORDS = ("TEXT", "VARCHAR", "CHAR", "STRING")
_DATE_KEYWORDS = ("DATE", "TIME", "TIMESTAMP")
_BOOL_KEYWORDS = ("BOOL", "BOOLEAN")


def _category_of(type_str: str) -> str:
    """Devuelve una de NUMERIC, TEXT, DATE, BOOLEAN, OTHER."""
    t = type_str.upper()
    if any(k in t for k in _NUMERIC_KEYWORDS):
        return "NUMERIC"
    if any(k in t for k in _BOOL_KEYWORDS):
        return "BOOLEAN"
    if any(k in t for k in _TEXT_KEYWORDS):
        return "TEXT"
    if any(k in t for k in _DATE_KEYWORDS):
        return "DATE"
    return "OTHER"


# ---------------------------------------------------------------------------
# Scope: bindings visibles en un nivel de consulta
# ---------------------------------------------------------------------------


@dataclass
class Scope:
    """Bindings visibles en una consulta o sub-consulta.

    ``relational_bindings`` mapea aliases (o nombres de tabla cuando no hay
    alias) a sus ``TableSchema``. ``graph_vertex_bindings`` cumple el rol
    análogo para variables ligadas dentro de un bloque MATCH; cada una
    enlaza a la ``TableSchema`` del backing table del label de vértice.
    """

    parent: Optional["Scope"] = None
    relational_bindings: dict[str, TableSchema] = field(default_factory=dict)
    graph_vertex_bindings: dict[str, TableSchema] = field(default_factory=dict)
    select_aliases: set[str] = field(default_factory=set)
    # Salida de un FromGraphMatch (alias → columnas en la cláusula COLUMNS)
    graph_output_columns: dict[str, frozenset[str]] = field(default_factory=dict)

    # ------------------------------------------------------------------
    # Lookups
    # ------------------------------------------------------------------

    def find_qualifier(self, qualifier: str) -> Optional[TableSchema]:
        """Busca el qualifier en bindings relacionales. ``None`` si no se
        encuentra acá ni en scopes padres."""
        target = qualifier.lower()
        for k, v in self.relational_bindings.items():
            if k.lower() == target:
                return v
        if self.parent is not None:
            return self.parent.find_qualifier(qualifier)
        return None

    def find_vertex_var(self, var: str) -> Optional[TableSchema]:
        target = var.lower()
        for k, v in self.graph_vertex_bindings.items():
            if k.lower() == target:
                return v
        if self.parent is not None:
            return self.parent.find_vertex_var(var)
        return None

    def find_graph_alias(self, alias: str) -> Optional[frozenset[str]]:
        target = alias.lower()
        for k, v in self.graph_output_columns.items():
            if k.lower() == target:
                return v
        if self.parent is not None:
            return self.parent.find_graph_alias(alias)
        return None

    def column_visible_unqualified(self, name: str) -> bool:
        target = name.lower()
        for ts in self.relational_bindings.values():
            if ts.find_column(target) is not None:
                return True
        for ts in self.graph_vertex_bindings.values():
            if ts.find_column(target) is not None:
                return True
        for cols in self.graph_output_columns.values():
            if target in {c.lower() for c in cols}:
                return True
        if target in {a.lower() for a in self.select_aliases}:
            return True
        if self.parent is not None:
            return self.parent.column_visible_unqualified(name)
        return False


# ---------------------------------------------------------------------------
# Verificador
# ---------------------------------------------------------------------------


class StructuralVerifier:
    def __init__(self, schema: ProjectedSchema):
        self.schema = schema
        self.errors: list[VerificationError] = []

    # API top-level --------------------------------------------------------

    def verify(self, query: ir.Query) -> list[VerificationError]:
        self.errors = []
        self._verify_query(query, parent=None)
        return list(self.errors)

    # Despacho de queries --------------------------------------------------

    def _verify_query(self, q: ir.Query, parent: Optional[Scope]) -> Scope:
        if isinstance(q, ir.RelationalQuery):
            return self._verify_relational(q, parent)
        if isinstance(q, ir.SetOperation):
            self._verify_query(q.left, parent)
            self._verify_query(q.right, parent)
            return Scope(parent=parent)
        # No soportamos otros tipos en esta rebanada
        return Scope(parent=parent)

    def _verify_relational(
        self, q: ir.RelationalQuery, parent: Optional[Scope]
    ) -> Scope:
        scope = Scope(parent=parent)

        if q.from_ is not None:
            self._bind_from(q.from_, scope)

        if q.where is not None:
            self._verify_expr(q.where, scope)

        for e in q.group_by:
            self._verify_expr(e, scope)

        # SELECT — el parser permite que un campo de tipo SelectItem llegue
        # como otro nodo IR (ej. Star directo) si el JSON no respeta el
        # schema; lo flageamos en lugar de crashear.
        for item in q.select:
            if not isinstance(item, ir.SelectItem):
                self.errors.append(
                    VerificationError(
                        kind="malformed_select_item",
                        message=(
                            f"item de SELECT no es un SelectItem; "
                            f"recibido {type(item).__name__}"
                        ),
                    )
                )
                continue
            self._verify_expr(item.expr, scope)
            if item.alias:
                scope.select_aliases.add(item.alias)

        if q.having is not None:
            self._verify_expr(q.having, scope)

        for o in q.order_by:
            if not isinstance(o, ir.OrderItem):
                self.errors.append(
                    VerificationError(
                        kind="malformed_order_item",
                        message=(
                            f"item de ORDER BY no es un OrderItem; "
                            f"recibido {type(o).__name__}"
                        ),
                    )
                )
                continue
            self._verify_expr(o.expr, scope)

        return scope

    # FROM y JOIN ----------------------------------------------------------

    def _bind_from(self, f: ir.FromExpr, scope: Scope) -> None:
        if isinstance(f, ir.FromTable):
            self._bind_from_table(f, scope)
        elif isinstance(f, ir.FromSubquery):
            self._bind_from_subquery(f, scope)
        elif isinstance(f, ir.Join):
            self._bind_from(f.left, scope)
            self._bind_from(f.right, scope)
            if f.on is not None:
                # ``on`` se verifica con el scope ya poblado por ambos lados
                self._verify_expr(f.on, scope)
        elif isinstance(f, ir.FromGraphMatch):
            self._bind_from_graph_match(f, scope)

    def _bind_from_table(self, f: ir.FromTable, scope: Scope) -> None:
        ts = self.schema.relational.find_table(f.table.name)
        if ts is None:
            self.errors.append(
                VerificationError(
                    kind="unknown_table",
                    message=f"tabla {f.table.name!r} no existe en el esquema",
                )
            )
            return
        binding_name = f.table.alias or f.table.name
        scope.relational_bindings[binding_name] = ts

    def _bind_from_subquery(self, f: ir.FromSubquery, scope: Scope) -> None:
        # Verificar la sub-consulta en su propio scope hijo
        sub_scope = self._verify_query(f.query, parent=scope)
        # Exponer el alias como una "tabla virtual" con las columnas del SELECT
        # del primer RelationalQuery alcanzable (las UNION/INTERSECT/EXCEPT
        # exponen el esquema de su operando izquierdo).
        items = _subquery_output_items(f.query)
        synthetic = TableSchema(
            name=f.alias,
            columns=tuple(
                ColumnSchema(
                    name=item.alias or _expr_implicit_name(item.expr) or "_",
                    type="OTHER",
                )
                for item in items
                if (item.alias or _expr_implicit_name(item.expr))
            ),
        )
        scope.relational_bindings[f.alias] = synthetic
        # también borramos warning de unused: sub_scope ya se usó dentro de verify_query
        del sub_scope

    def _bind_from_graph_match(self, f: ir.FromGraphMatch, scope: Scope) -> None:
        graph = self.schema.find_graph(f.match.graph)
        if graph is None:
            self.errors.append(
                VerificationError(
                    kind="unknown_graph",
                    message=f"property graph {f.match.graph!r} no declarado",
                )
            )
            # Igual seguimos para reportar el resto de errores estructurales
            graph = None  # type: ignore[assignment]

        # Scope local al MATCH: vertex/edge variables son visibles en
        # COLUMNS y en el WHERE interno del bloque
        match_scope = Scope(parent=scope)

        for path in f.match.patterns:
            self._bind_vertex(path.head, graph, match_scope)
            prev_vertex = path.head
            for edge, vertex in path.steps:
                self._bind_edge(edge, graph, match_scope)
                self._bind_vertex(vertex, graph, match_scope)
                self._check_path_step_coherence(prev_vertex, edge, vertex, graph)
                prev_vertex = vertex

        if f.match.where is not None:
            self._verify_expr(f.match.where, match_scope)

        for col in f.match.columns:
            self._verify_expr(col.expr, match_scope)

        # El alias externo (si existe) expone las columnas declaradas en
        # COLUMNS hacia el scope relacional padre
        out_cols = frozenset(
            (c.alias or _expr_implicit_name(c.expr) or "")
            for c in f.match.columns
            if (c.alias or _expr_implicit_name(c.expr))
        )
        binding_name = f.alias or f.match.graph
        scope.graph_output_columns[binding_name] = out_cols

    def _bind_vertex(
        self,
        v: ir.VertexPattern,
        graph: Optional[PropertyGraphSchema],
        scope: Scope,
    ) -> None:
        if v.label is None:
            # Patrón sin label: variable se enlaza a "any vertex"; saltamos
            # el chequeo de existencia y tipos
            return
        if graph is None:
            return
        vt = graph.find_vertex_label(v.label)
        if vt is None:
            self.errors.append(
                VerificationError(
                    kind="unknown_vertex_label",
                    message=(
                        f"label de vértice {v.label!r} no declarado "
                        f"en el grafo {graph.name!r}"
                    ),
                )
            )
            return
        # Coherencia cruzada: el label tiene tabla pero ¿esa tabla existe?
        backing = self.schema.relational.find_table(vt.table)
        if backing is None:
            self.errors.append(
                VerificationError(
                    kind="vertex_label_without_table",
                    message=(
                        f"label {v.label!r} declara backing table {vt.table!r} "
                        f"que no existe en el esquema relacional"
                    ),
                )
            )
            return
        scope.graph_vertex_bindings[v.var] = backing

    def _bind_edge(
        self,
        e: ir.EdgePattern,
        graph: Optional[PropertyGraphSchema],
        scope: Scope,
    ) -> None:
        if e.label is None or graph is None:
            return
        et = graph.find_edge_label(e.label)
        if et is None:
            self.errors.append(
                VerificationError(
                    kind="unknown_edge_label",
                    message=(
                        f"label de arista {e.label!r} no declarado "
                        f"en el grafo {graph.name!r}"
                    ),
                )
            )

    def _check_path_step_coherence(
        self,
        prev_vertex: ir.VertexPattern,
        edge: ir.EdgePattern,
        next_vertex: ir.VertexPattern,
        graph: Optional[PropertyGraphSchema],
    ) -> None:
        """Cuarta clase de chequeo (motivada por el experimento siete).

        Verifica que en un step ``(prev_vertex)-[edge]->(next_vertex)`` los
        labels de los vértices sean compatibles con los ``source_label`` y
        ``destination_label`` declarados por el edge en el property graph.

        Se omite el chequeo cuando faltan datos para razonar (grafo
        inexistente, label de arista inexistente, vértice sin label
        anotado), ya que esos casos quedan reportados por sus chequeos
        propios y agregar acá ruido empeora el reporte.

        La dirección del edge en la IR controla qué orden se exige:

        - ``->``: prev_vertex.label debe ser source_label, next debe ser destination_label.
        - ``<-``: prev_vertex.label debe ser destination_label, next debe ser source_label.
        - ``-`` : cualquiera de las dos orientaciones es aceptable (no dirigido).
        """
        if graph is None or edge.label is None:
            return
        edge_decl = graph.find_edge_label(edge.label)
        if edge_decl is None:
            return
        if prev_vertex.label is None or next_vertex.label is None:
            return

        expected_src = edge_decl.source_label.lower()
        expected_dst = edge_decl.destination_label.lower()
        actual_prev = prev_vertex.label.lower()
        actual_next = next_vertex.label.lower()

        forward_ok = (actual_prev == expected_src and actual_next == expected_dst)
        backward_ok = (actual_prev == expected_dst and actual_next == expected_src)

        if edge.direction == "->":
            ok = forward_ok
            arrow = "->"
        elif edge.direction == "<-":
            ok = backward_ok
            arrow = "<-"
        else:
            ok = forward_ok or backward_ok
            arrow = "-"

        if not ok:
            self.errors.append(
                VerificationError(
                    kind="path_step_incoherent",
                    message=(
                        f"path step ({prev_vertex.var}:{prev_vertex.label})"
                        f"{arrow}[{edge.var}:{edge.label}]"
                        f"{arrow}({next_vertex.var}:{next_vertex.label}) "
                        f"incompatible con la declaración: {edge.label} es "
                        f"{edge_decl.source_label} → {edge_decl.destination_label}"
                    ),
                )
            )

    # Expresiones ----------------------------------------------------------

    def _verify_expr(self, e: ir.Expression, scope: Scope) -> None:
        """Recorre la expresión, valida referencias y tipos. No retorna nada;
        acumula errores en ``self.errors``."""
        if isinstance(e, ir.ColumnExpr):
            self._check_column_ref(e.ref, scope)
            return
        if isinstance(e, (ir.Literal, ir.Star)):
            return
        if isinstance(e, ir.ParenExpr):
            self._verify_expr(e.inner, scope)
            return
        if isinstance(e, ir.BinaryOp):
            self._verify_expr(e.left, scope)
            self._verify_expr(e.right, scope)
            self._check_arithmetic_types(e, scope)
            return
        if isinstance(e, ir.UnaryOp):
            self._verify_expr(e.operand, scope)
            return
        if isinstance(e, ir.Aggregate):
            for a in e.args:
                self._verify_expr(a, scope)
            self._check_aggregate_types(e, scope)
            return
        if isinstance(e, ir.FunctionCall):
            for a in e.args:
                self._verify_expr(a, scope)
            return
        if isinstance(e, ir.CaseExpr):
            for cond, val in e.branches:
                self._verify_expr(cond, scope)
                self._verify_expr(val, scope)
            if e.else_ is not None:
                self._verify_expr(e.else_, scope)
            return
        if isinstance(e, ir.CastExpr):
            self._verify_expr(e.expr, scope)
            return
        if isinstance(e, ir.LikeExpr):
            self._verify_expr(e.left, scope)
            self._verify_expr(e.pattern, scope)
            return
        if isinstance(e, ir.InExpr):
            self._verify_expr(e.left, scope)
            if isinstance(e.rhs, ir.Subquery):
                self._verify_query(e.rhs.query, parent=scope)
            else:
                for x in e.rhs:
                    self._verify_expr(x, scope)
            return
        if isinstance(e, ir.IsNullExpr):
            self._verify_expr(e.operand, scope)
            return
        if isinstance(e, ir.BetweenExpr):
            self._verify_expr(e.operand, scope)
            self._verify_expr(e.low, scope)
            self._verify_expr(e.high, scope)
            return
        if isinstance(e, ir.ExistsExpr):
            self._verify_query(e.query, parent=scope)
            return
        if isinstance(e, ir.Subquery):
            self._verify_query(e.query, parent=scope)
            return
        # Tipos no cubiertos: silencio. El compiler no los emite.

    # Resolución de columnas ----------------------------------------------

    def _check_column_ref(self, c: ir.ColumnRef, scope: Scope) -> None:
        if c.qualifier is None:
            if scope.column_visible_unqualified(c.name):
                return
            self.errors.append(
                VerificationError(
                    kind="unknown_column",
                    message=f"columna {c.name!r} no resuelve en ningún binding del scope",
                )
            )
            return

        # Qualifier presente: puede ser alias de tabla, de subquery, variable
        # de vértice o alias de un FromGraphMatch
        ts = scope.find_qualifier(c.qualifier)
        if ts is not None:
            if ts.find_column(c.name) is None:
                self.errors.append(
                    VerificationError(
                        kind="unknown_column",
                        message=(
                            f"columna {c.qualifier}.{c.name} — "
                            f"{c.name!r} no existe en {ts.name!r}"
                        ),
                    )
                )
            return

        ts_v = scope.find_vertex_var(c.qualifier)
        if ts_v is not None:
            if ts_v.find_column(c.name) is None:
                self.errors.append(
                    VerificationError(
                        kind="unknown_column",
                        message=(
                            f"propiedad {c.qualifier}.{c.name} — "
                            f"{c.name!r} no existe en backing {ts_v.name!r}"
                        ),
                    )
                )
            return

        out_cols = scope.find_graph_alias(c.qualifier)
        if out_cols is not None:
            if c.name.lower() not in {x.lower() for x in out_cols}:
                self.errors.append(
                    VerificationError(
                        kind="unknown_column",
                        message=(
                            f"columna {c.qualifier}.{c.name} no fue declarada "
                            f"en la cláusula COLUMNS del bloque GRAPH_TABLE"
                        ),
                    )
                )
            return

        self.errors.append(
            VerificationError(
                kind="unknown_qualifier",
                message=f"qualifier {c.qualifier!r} no resuelve a tabla, vértice ni alias de bloque de grafo",
            )
        )

    # Type checking --------------------------------------------------------

    def _check_aggregate_types(self, agg: ir.Aggregate, scope: Scope) -> None:
        """``AVG`` y ``SUM`` solo aceptan operando numérico."""
        if agg.name not in {"AVG", "SUM"}:
            return
        if not agg.args:
            return
        cat = self._category_of_expr(agg.args[0], scope)
        if cat in {"NUMERIC", "ANY", "UNKNOWN"}:
            return
        self.errors.append(
            VerificationError(
                kind="type_mismatch_aggregate",
                message=(
                    f"agregación {agg.name} requiere operando NUMERIC, "
                    f"recibió {cat}"
                ),
            )
        )

    def _check_arithmetic_types(self, b: ir.BinaryOp, scope: Scope) -> None:
        """Los operadores aritméticos requieren NUMERIC en ambos operandos."""
        if b.op not in {"+", "-", "*", "/", "%"}:
            return
        lc = self._category_of_expr(b.left, scope)
        rc = self._category_of_expr(b.right, scope)
        for cat in (lc, rc):
            if cat not in {"NUMERIC", "ANY", "UNKNOWN"}:
                self.errors.append(
                    VerificationError(
                        kind="type_mismatch_arithmetic",
                        message=(
                            f"operador aritmético {b.op!r} requiere operandos NUMERIC; "
                            f"se obtuvo lhs={lc}, rhs={rc}"
                        ),
                    )
                )
                return  # un solo error por BinaryOp

    def _category_of_expr(self, e: ir.Expression, scope: Scope) -> str:
        """Inferencia de categoría de tipo. Devuelve NUMERIC/TEXT/DATE/
        BOOLEAN/OTHER/ANY/UNKNOWN."""
        if isinstance(e, ir.Literal):
            v = e.value
            if isinstance(v, bool):
                return "BOOLEAN"
            if isinstance(v, (int, float)):
                return "NUMERIC"
            if isinstance(v, str):
                return "TEXT"
            return "ANY"
        if isinstance(e, ir.ColumnExpr):
            cs = self._resolve_column_schema(e.ref, scope)
            if cs is None:
                return "UNKNOWN"
            return _category_of(cs.type)
        if isinstance(e, ir.Aggregate):
            if e.name in {"COUNT"}:
                return "NUMERIC"
            if e.name in {"SUM", "AVG"}:
                return "NUMERIC"
            if e.name in {"MIN", "MAX"} and e.args:
                return self._category_of_expr(e.args[0], scope)
            return "ANY"
        if isinstance(e, ir.BinaryOp):
            if e.op in {"+", "-", "*", "/", "%"}:
                return "NUMERIC"
            if e.op in {"=", "<>", "<", "<=", ">", ">=", "AND", "OR"}:
                return "BOOLEAN"
            return "ANY"
        if isinstance(e, ir.CastExpr):
            return _category_of(e.type_name)
        if isinstance(e, ir.ParenExpr):
            return self._category_of_expr(e.inner, scope)
        return "ANY"

    def _resolve_column_schema(
        self, c: ir.ColumnRef, scope: Scope
    ) -> Optional[ColumnSchema]:
        """Devuelve la ``ColumnSchema`` asociada a ``c`` si se puede resolver,
        ``None`` en caso contrario. No genera errores aquí; eso lo hace
        ``_check_column_ref``."""
        if c.qualifier is not None:
            ts = scope.find_qualifier(c.qualifier) or scope.find_vertex_var(c.qualifier)
            if ts is None:
                return None
            return ts.find_column(c.name)

        # Sin qualifier: buscar en todas las tablas del scope
        s: Optional[Scope] = scope
        while s is not None:
            for ts in s.relational_bindings.values():
                cs = ts.find_column(c.name)
                if cs is not None:
                    return cs
            for ts in s.graph_vertex_bindings.values():
                cs = ts.find_column(c.name)
                if cs is not None:
                    return cs
            s = s.parent
        return None


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _expr_implicit_name(e: ir.Expression) -> Optional[str]:
    """Nombre implícito de una expresión cuando no tiene alias.

    Por convención SQL, una columna desnuda preserva su nombre; lo demás no
    tiene un nombre canónico y se ignora aquí."""
    if isinstance(e, ir.ColumnExpr):
        return e.ref.name
    return None


def _subquery_output_items(q: ir.Query) -> tuple[ir.SelectItem, ...]:
    """Devuelve los SelectItem que definen el esquema de salida de una query.

    Para una ``RelationalQuery``, son sus propios items; para una
    ``SetOperation`` (UNION/INTERSECT/EXCEPT), por convención SQL los nombres
    de columnas vienen del operando izquierdo."""
    if isinstance(q, ir.RelationalQuery):
        return q.select
    if isinstance(q, ir.SetOperation):
        return _subquery_output_items(q.left)
    return ()


# ---------------------------------------------------------------------------
# API funcional pública
# ---------------------------------------------------------------------------


def verify_ir(query: ir.Query, schema: ProjectedSchema) -> list[VerificationError]:
    """Verifica una consulta IR-SQL/PGQ contra un esquema proyectado.

    Devuelve la lista (posiblemente vacía) de errores estructurales
    detectados. La lista vacía indica que la consulta es estructuralmente
    válida, no que su semántica sea correcta ni que vaya a ejecutar sin error.
    """
    return StructuralVerifier(schema).verify(query)
