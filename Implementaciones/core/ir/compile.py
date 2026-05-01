"""
Compilador determinista IR-SQL/PGQ → SQL textual.

Recorre el árbol de la IR de forma ascendente y emite la consulta como string.
La invariante central (capítulo 4 §4.5.3 de la tesis) es:

    dos instancias estructuralmente idénticas de la IR producen el mismo SQL.

Eso se cumple porque el compilador no consulta estado externo y porque las
dataclasses de la IR son ``frozen=True`` (igualdad estructural ⇒ igualdad
hashable ⇒ output idéntico).

El compilador NO optimiza. Inserta paréntesis liberalmente alrededor de
operadores binarios y subconsultas para no depender de la precedencia
implícita del dialecto.

Esta rebanada implementa el bloque relacional. Los bloques de grafo y la
composición híbrida quedan para rebanadas 2 y 3 respectivamente.
"""

from __future__ import annotations

from . import nodes as ir


# ---------------------------------------------------------------------------
# Punto de entrada
# ---------------------------------------------------------------------------


def compile_query(query: ir.Query) -> str:
    return _compile_query(query)


# ---------------------------------------------------------------------------
# Queries
# ---------------------------------------------------------------------------


def _compile_query(q: ir.Query) -> str:
    if isinstance(q, ir.RelationalQuery):
        return _compile_relational(q)
    if isinstance(q, ir.SetOperation):
        return _compile_set_op(q)
    raise NotImplementedError(f"compile_query: tipo no soportado {type(q).__name__}")


def _compile_set_op(s: ir.SetOperation) -> str:
    return f"({_compile_query(s.left)}) {s.op} ({_compile_query(s.right)})"


def _compile_relational(q: ir.RelationalQuery) -> str:
    parts: list[str] = []
    head = "SELECT DISTINCT " if q.distinct else "SELECT "
    head += ", ".join(_compile_select_item(i) for i in q.select)
    parts.append(head)

    if q.from_ is not None:
        parts.append("FROM " + _compile_from(q.from_))
    if q.where is not None:
        parts.append("WHERE " + _compile_expr(q.where))
    if q.group_by:
        parts.append("GROUP BY " + ", ".join(_compile_expr(e) for e in q.group_by))
    if q.having is not None:
        parts.append("HAVING " + _compile_expr(q.having))
    if q.order_by:
        parts.append("ORDER BY " + ", ".join(_compile_order(o) for o in q.order_by))
    if q.limit is not None:
        parts.append(f"LIMIT {q.limit}")
    if q.offset is not None:
        parts.append(f"OFFSET {q.offset}")
    return " ".join(parts)


def _compile_select_item(item: ir.SelectItem) -> str:
    body = _compile_expr(item.expr)
    if item.alias:
        return f"{body} AS {item.alias}"
    return body


# ---------------------------------------------------------------------------
# FROM y JOIN
# ---------------------------------------------------------------------------


def _compile_from(f: ir.FromExpr) -> str:
    if isinstance(f, ir.FromTable):
        return _compile_table_ref(f.table)
    if isinstance(f, ir.FromSubquery):
        return f"({_compile_query(f.query)}) AS {f.alias}"
    if isinstance(f, ir.Join):
        return _compile_join(f)
    if isinstance(f, ir.FromGraphMatch):
        return _compile_from_graph_match(f)
    raise NotImplementedError(f"compile_from: tipo no soportado {type(f).__name__}")


# ---------------------------------------------------------------------------
# Bloques de grafo (rebanada 2)
# ---------------------------------------------------------------------------


def _compile_from_graph_match(f: ir.FromGraphMatch) -> str:
    """DuckPGQ no acepta el keyword ``AS`` entre ``GRAPH_TABLE(...)`` y el
    alias; el alias va separado solo por un espacio."""
    body = f"GRAPH_TABLE ({_compile_match_pattern(f.match)})"
    if f.alias:
        return f"{body} {f.alias}"
    return body


def _compile_match_pattern(m: ir.MatchPattern) -> str:
    parts = [m.graph, "MATCH"]
    parts.append(", ".join(_compile_path_pattern(p) for p in m.patterns))
    if m.where is not None:
        parts.append("WHERE " + _compile_expr(m.where))
    cols = ", ".join(_compile_select_item(c) for c in m.columns)
    parts.append(f"COLUMNS ({cols})")
    return " ".join(parts)


def _compile_path_pattern(p: ir.PathPattern) -> str:
    parts = [_compile_vertex_pattern(p.head)]
    for edge, vertex in p.steps:
        parts.append(_compile_edge_pattern(edge))
        parts.append(_compile_vertex_pattern(vertex))
    return "".join(parts)


def _compile_vertex_pattern(v: ir.VertexPattern) -> str:
    label = f":{v.label}" if v.label else ""
    return f"({v.var}{label})"


def _compile_edge_pattern(e: ir.EdgePattern) -> str:
    """DuckPGQ exige variable explícita en el edge incluso cuando no se usa.

    El compilador preserva la dirección que la IR declara: ``->`` (hacia adelante),
    ``<-`` (hacia atrás) y ``-`` (no dirigida).
    """
    label = f":{e.label}" if e.label else ""
    body = f"[{e.var}{label}]"
    if e.direction == "->":
        return f"-{body}->"
    if e.direction == "<-":
        return f"<-{body}-"
    if e.direction == "-":
        return f"-{body}-"
    raise NotImplementedError(f"_compile_edge_pattern: dirección desconocida {e.direction!r}")


def _compile_join(j: ir.Join) -> str:
    left = _compile_from(j.left)
    right = _compile_from(j.right)
    kind = j.kind.upper()
    if kind == "CROSS":
        return f"{left} CROSS JOIN {right}"
    if kind == "NATURAL":
        return f"{left} NATURAL JOIN {right}"
    body = f"{left} {kind} JOIN {right}"
    if j.on is not None:
        body += f" ON {_compile_expr(j.on)}"
    elif j.using:
        body += f" USING ({', '.join(j.using)})"
    return body


def _compile_table_ref(t: ir.TableRef) -> str:
    if t.alias and t.alias != t.name:
        return f"{t.name} AS {t.alias}"
    return t.name


def _compile_order(o: ir.OrderItem) -> str:
    out = f"{_compile_expr(o.expr)} {o.direction.upper()}"
    if o.nulls:
        out += f" NULLS {o.nulls.upper()}"
    return out


# ---------------------------------------------------------------------------
# Expresiones
# ---------------------------------------------------------------------------


def _compile_expr(e: ir.Expression) -> str:
    if isinstance(e, ir.Literal):
        return _compile_literal(e)
    if isinstance(e, ir.ColumnExpr):
        return _compile_column(e.ref)
    if isinstance(e, ir.Star):
        return f"{e.qualifier}.*" if e.qualifier else "*"
    if isinstance(e, ir.BinaryOp):
        return f"({_compile_expr(e.left)} {e.op} {_compile_expr(e.right)})"
    if isinstance(e, ir.UnaryOp):
        if e.op.upper() == "NOT":
            return f"(NOT {_compile_expr(e.operand)})"
        return f"({e.op}{_compile_expr(e.operand)})"
    if isinstance(e, ir.FunctionCall):
        args = ", ".join(_compile_expr(a) for a in e.args)
        return f"{e.name}({args})"
    if isinstance(e, ir.Aggregate):
        prefix = "DISTINCT " if e.distinct else ""
        args = ", ".join(_compile_expr(a) for a in e.args) if e.args else ""
        return f"{e.name}({prefix}{args})"
    if isinstance(e, ir.CaseExpr):
        return _compile_case(e)
    if isinstance(e, ir.CastExpr):
        return f"CAST({_compile_expr(e.expr)} AS {e.type_name})"
    if isinstance(e, ir.LikeExpr):
        op = "NOT LIKE" if e.negate else "LIKE"
        return f"({_compile_expr(e.left)} {op} {_compile_expr(e.pattern)})"
    if isinstance(e, ir.InExpr):
        return _compile_in(e)
    if isinstance(e, ir.IsNullExpr):
        op = "IS NOT NULL" if e.negate else "IS NULL"
        return f"({_compile_expr(e.operand)} {op})"
    if isinstance(e, ir.BetweenExpr):
        op = "NOT BETWEEN" if e.negate else "BETWEEN"
        return (
            f"({_compile_expr(e.operand)} {op} "
            f"{_compile_expr(e.low)} AND {_compile_expr(e.high)})"
        )
    if isinstance(e, ir.ExistsExpr):
        op = "NOT EXISTS" if e.negate else "EXISTS"
        return f"{op} ({_compile_query(e.query)})"
    if isinstance(e, ir.Subquery):
        return f"({_compile_query(e.query)})"
    if isinstance(e, ir.ParenExpr):
        return f"({_compile_expr(e.inner)})"
    raise NotImplementedError(f"compile_expr: tipo no soportado {type(e).__name__}")


def _compile_column(c: ir.ColumnRef) -> str:
    return f"{c.qualifier}.{c.name}" if c.qualifier else c.name


def _compile_literal(lit: ir.Literal) -> str:
    if lit.raw is not None:
        return lit.raw
    v = lit.value
    if v is None:
        return "NULL"
    if isinstance(v, bool):
        return "TRUE" if v else "FALSE"
    if isinstance(v, (int, float)):
        return str(v)
    if isinstance(v, str):
        escaped = v.replace("'", "''")
        return f"'{escaped}'"
    raise NotImplementedError(f"compile_literal: valor no soportado {type(v).__name__}")


def _compile_case(c: ir.CaseExpr) -> str:
    parts = ["CASE"]
    for cond, val in c.branches:
        parts.append(f"WHEN {_compile_expr(cond)} THEN {_compile_expr(val)}")
    if c.else_ is not None:
        parts.append(f"ELSE {_compile_expr(c.else_)}")
    parts.append("END")
    return " ".join(parts)


def _compile_in(e: ir.InExpr) -> str:
    op = "NOT IN" if e.negate else "IN"
    if isinstance(e.rhs, ir.Subquery):
        rhs = f"({_compile_query(e.rhs.query)})"
    else:
        rhs = "(" + ", ".join(_compile_expr(x) for x in e.rhs) + ")"
    return f"({_compile_expr(e.left)} {op} {rhs})"
