"""
Lifter sqlglot AST → IR-SQL/PGQ.

El lifter recibe SQL textual y devuelve una instancia de la IR. Es la
herramienta central de la rebanada 1: nos permite construir IR a partir de
las predicciones del LLM (sin obligar al modelo a emitir IR directamente,
trabajo que corresponde a la rebanada 5) y validar la expresividad de la IR
contra corpus reales.

El lifter está acoplado a sqlglot por construcción. La IR no lo está: el
módulo ``core.ir.nodes`` no importa sqlglot. Ese acoplamiento es deseable y
está aislado en este archivo.

Cobertura prevista: SELECT con FROM, JOINs (INNER/LEFT/RIGHT/CROSS), WHERE,
GROUP BY, HAVING, ORDER BY, LIMIT, set operations (UNION/INTERSECT/EXCEPT),
subconsultas, expresiones aritméticas y de comparación, IN, EXISTS, LIKE,
BETWEEN, IS NULL, CASE, CAST, agregaciones y funciones escalares anónimas.
Lo no cubierto eleva ``UnsupportedSQLError``.
"""

from __future__ import annotations

import sqlglot
from sqlglot import exp

from . import nodes as ir


class UnsupportedSQLError(NotImplementedError):
    """Se eleva cuando el lifter encuentra un nodo sqlglot que aún no soporta.

    El mensaje incluye el nombre de la clase sqlglot, lo que facilita iterar
    el lifter contra corpus nuevos.
    """


# ---------------------------------------------------------------------------
# Punto de entrada
# ---------------------------------------------------------------------------


def lift_sql(sql: str, dialect: str = "sqlite") -> ir.Query:
    parsed = sqlglot.parse_one(sql, dialect=dialect)
    return _lift_query(parsed)


# ---------------------------------------------------------------------------
# Queries
# ---------------------------------------------------------------------------


def _lift_query(node: exp.Expression) -> ir.Query:
    if isinstance(node, exp.Select):
        return _lift_select(node)
    if isinstance(node, exp.Union):
        return _lift_set_op(node, "UNION")
    if isinstance(node, exp.Intersect):
        return _lift_set_op(node, "INTERSECT")
    if isinstance(node, exp.Except):
        return _lift_set_op(node, "EXCEPT")
    if isinstance(node, exp.Subquery):
        return _lift_query(node.this)
    raise UnsupportedSQLError(f"_lift_query: {type(node).__name__}")


def _lift_set_op(node: exp.Union | exp.Intersect | exp.Except, base: str) -> ir.SetOperation:
    op = base + (" ALL" if node.args.get("distinct") is False else "")
    return ir.SetOperation(
        op=op,
        left=_lift_query(node.left),
        right=_lift_query(node.right),
    )


def _lift_select(node: exp.Select) -> ir.RelationalQuery:
    select_items = tuple(_lift_select_item(s) for s in node.expressions)
    from_clause = node.args.get("from_") or node.args.get("from")
    joins = node.args.get("joins") or []

    from_ir: ir.FromExpr | None = None
    if from_clause is not None:
        # exp.From wraps a single source expression (table or subquery); joins
        # come as a separate list at the Select level in sqlglot.
        from_ir = _lift_from_source(from_clause.this)
        for j in joins:
            from_ir = _lift_join(from_ir, j)

    where_node = node.args.get("where")
    where_ir = _lift_expr(where_node.this) if where_node is not None else None

    group_node = node.args.get("group")
    group_ir: tuple[ir.Expression, ...] = ()
    if group_node is not None:
        group_ir = tuple(_lift_expr(e) for e in group_node.expressions)

    having_node = node.args.get("having")
    having_ir = _lift_expr(having_node.this) if having_node is not None else None

    order_node = node.args.get("order")
    order_ir: tuple[ir.OrderItem, ...] = ()
    if order_node is not None:
        order_ir = tuple(_lift_order(o) for o in order_node.expressions)

    limit_node = node.args.get("limit")
    limit = _lift_int_arg(limit_node.expression) if limit_node is not None else None

    offset_node = node.args.get("offset")
    offset = _lift_int_arg(offset_node.expression) if offset_node is not None else None

    distinct = bool(node.args.get("distinct"))

    return ir.RelationalQuery(
        select=select_items,
        from_=from_ir,
        where=where_ir,
        group_by=group_ir,
        having=having_ir,
        order_by=order_ir,
        limit=limit,
        offset=offset,
        distinct=distinct,
    )


def _lift_int_arg(node: exp.Expression) -> int | None:
    if isinstance(node, exp.Literal) and node.is_int:
        return int(node.name)
    if isinstance(node, exp.Literal):
        try:
            return int(node.name)
        except ValueError:
            return None
    return None


# ---------------------------------------------------------------------------
# SELECT items
# ---------------------------------------------------------------------------


def _lift_select_item(node: exp.Expression) -> ir.SelectItem:
    if isinstance(node, exp.Alias):
        return ir.SelectItem(expr=_lift_expr(node.this), alias=node.alias)
    return ir.SelectItem(expr=_lift_expr(node))


# ---------------------------------------------------------------------------
# FROM y JOIN
# ---------------------------------------------------------------------------


def _lift_from_source(node: exp.Expression) -> ir.FromExpr:
    if isinstance(node, exp.Table):
        return ir.FromTable(
            table=ir.TableRef(name=node.name, alias=node.alias or None)
        )
    if isinstance(node, exp.Subquery):
        return ir.FromSubquery(
            query=_lift_query(node.this),
            alias=node.alias or "_sub",
        )
    raise UnsupportedSQLError(f"_lift_from_source: {type(node).__name__}")


def _lift_join(left: ir.FromExpr, j: exp.Join) -> ir.Join:
    right = _lift_from_source(j.this)
    side = j.args.get("side") or ""
    kind_word = j.args.get("kind") or ""
    if side and kind_word:
        kind = f"{side} {kind_word}".strip()
    elif side:
        kind = side
    elif kind_word:
        kind = kind_word
    else:
        kind = "INNER"
    on = j.args.get("on")
    using = j.args.get("using")
    on_ir = _lift_expr(on) if on is not None else None
    using_ir: tuple[str, ...] | None = None
    if using:
        using_ir = tuple(c.name for c in using)
    return ir.Join(left=left, right=right, kind=kind, on=on_ir, using=using_ir)


# ---------------------------------------------------------------------------
# ORDER BY items
# ---------------------------------------------------------------------------


def _lift_order(node: exp.Expression) -> ir.OrderItem:
    if isinstance(node, exp.Ordered):
        direction = "DESC" if node.args.get("desc") else "ASC"
        nulls = None
        if node.args.get("nulls_first"):
            nulls = "FIRST"
        elif node.args.get("nulls_last"):
            nulls = "LAST"
        return ir.OrderItem(
            expr=_lift_expr(node.this),
            direction=direction,
            nulls=nulls,
        )
    return ir.OrderItem(expr=_lift_expr(node))


# ---------------------------------------------------------------------------
# Expresiones
# ---------------------------------------------------------------------------


_BIN_OP_MAP: dict[type[exp.Expression], str] = {
    exp.EQ: "=",
    exp.NEQ: "<>",
    exp.LT: "<",
    exp.LTE: "<=",
    exp.GT: ">",
    exp.GTE: ">=",
    exp.Add: "+",
    exp.Sub: "-",
    exp.Mul: "*",
    exp.Div: "/",
    exp.Mod: "%",
    exp.And: "AND",
    exp.Or: "OR",
}


_AGG_NAMES: dict[type[exp.Expression], str] = {
    exp.Count: "COUNT",
    exp.Sum: "SUM",
    exp.Avg: "AVG",
    exp.Max: "MAX",
    exp.Min: "MIN",
}


def _lift_expr(node: exp.Expression) -> ir.Expression:
    # Subqueries en posición de expresión
    if isinstance(node, exp.Subquery):
        return ir.Subquery(query=_lift_query(node.this))

    # Paréntesis explícitos
    if isinstance(node, exp.Paren):
        return ir.ParenExpr(inner=_lift_expr(node.this))

    # Literales y constantes
    if isinstance(node, exp.Null):
        return ir.Literal(value=None, raw="NULL")
    if isinstance(node, exp.Boolean):
        return ir.Literal(value=bool(node.this), raw="TRUE" if node.this else "FALSE")
    if isinstance(node, exp.Literal):
        return _lift_literal(node)

    # Referencias
    if isinstance(node, exp.Column):
        qualifier = node.table or None
        return ir.ColumnExpr(ref=ir.ColumnRef(name=node.name, qualifier=qualifier))
    if isinstance(node, exp.Star):
        return ir.Star()
    if isinstance(node, exp.Dot):
        # `t.*` aparece como exp.Dot(this=Column(t), expression=Star)
        if isinstance(node.expression, exp.Star):
            qual = node.this.name if isinstance(node.this, (exp.Column, exp.Identifier)) else None
            return ir.Star(qualifier=qual)
        # Cualquier otro Dot lo tratamos como ColumnRef calificado
        if isinstance(node.this, (exp.Column, exp.Identifier)) and isinstance(
            node.expression, (exp.Identifier, exp.Column)
        ):
            return ir.ColumnExpr(
                ref=ir.ColumnRef(
                    name=node.expression.name,
                    qualifier=node.this.name,
                )
            )
        raise UnsupportedSQLError(f"_lift_expr Dot variante: {node.sql()}")

    # Operadores binarios
    bin_op = _BIN_OP_MAP.get(type(node))
    if bin_op is not None:
        return ir.BinaryOp(
            op=bin_op,
            left=_lift_expr(node.this),
            right=_lift_expr(node.expression),
        )

    # Operadores unarios
    if isinstance(node, exp.Not):
        return ir.UnaryOp(op="NOT", operand=_lift_expr(node.this))
    if isinstance(node, exp.Neg):
        return ir.UnaryOp(op="-", operand=_lift_expr(node.this))

    # Comparaciones especiales
    if isinstance(node, exp.Is):
        target = node.expression
        if isinstance(target, exp.Null):
            return ir.IsNullExpr(operand=_lift_expr(node.this), negate=False)
        # exp.Is con not expression: x IS NOT NULL aparece como Not(Is(x, Null))
        raise UnsupportedSQLError(f"_lift_expr Is no-NULL: {node.sql()}")
    if isinstance(node, exp.Like):
        return ir.LikeExpr(
            left=_lift_expr(node.this),
            pattern=_lift_expr(node.expression),
            negate=False,
        )
    if isinstance(node, exp.ILike):
        # ILike no es estándar; emitimos como LIKE para máxima portabilidad
        return ir.LikeExpr(
            left=_lift_expr(node.this),
            pattern=_lift_expr(node.expression),
            negate=False,
        )
    if isinstance(node, exp.In):
        return _lift_in(node)
    if isinstance(node, exp.Between):
        return ir.BetweenExpr(
            operand=_lift_expr(node.this),
            low=_lift_expr(node.args["low"]),
            high=_lift_expr(node.args["high"]),
            negate=False,
        )
    if isinstance(node, exp.Exists):
        return ir.ExistsExpr(query=_lift_query(node.this), negate=False)

    # CASE / IF
    if isinstance(node, exp.Case):
        return _lift_case(node)
    if isinstance(node, exp.If):
        # IF(cond, then, else) → CASE WHEN cond THEN then ELSE else END
        cond = _lift_expr(node.this)
        then = _lift_expr(node.args["true"])
        else_ = _lift_expr(node.args["false"]) if node.args.get("false") is not None else None
        return ir.CaseExpr(branches=((cond, then),), else_=else_)

    # CAST
    if isinstance(node, exp.Cast):
        return ir.CastExpr(
            expr=_lift_expr(node.this),
            type_name=node.to.sql(dialect="sqlite").upper(),
        )

    # DISTINCT como expresión (raro, pero sqlglot lo emite dentro de COUNT)
    if isinstance(node, exp.Distinct):
        # Un único hijo distinto se trata como agregación con distinct=True;
        # no debería aparecer fuera de un Aggregate.
        if len(node.expressions) == 1:
            return _lift_expr(node.expressions[0])
        raise UnsupportedSQLError(f"_lift_expr Distinct multiple: {node.sql()}")

    # Agregaciones específicas
    agg_name = _AGG_NAMES.get(type(node))
    if agg_name is not None:
        return _lift_aggregate(agg_name, node)

    # Función agregada genérica (heredada de AggFunc)
    if isinstance(node, exp.AggFunc):
        return _lift_aggregate(_normalize_func_name(node), node)

    # Función escalar genérica
    if isinstance(node, (exp.Func, exp.Anonymous)):
        return _lift_function(node)

    raise UnsupportedSQLError(f"_lift_expr: {type(node).__name__} ({node.sql()})")


def _normalize_func_name(node: exp.Expression) -> str:
    if isinstance(node, exp.Anonymous):
        return node.name.upper()
    return type(node).__name__.upper()


def _lift_literal(node: exp.Literal) -> ir.Literal:
    raw = node.name
    if node.is_int:
        return ir.Literal(value=int(raw), raw=str(int(raw)))
    if node.is_number:
        return ir.Literal(value=float(raw), raw=raw)
    return ir.Literal(value=raw, raw=f"'{raw.replace(chr(39), chr(39) * 2)}'")


def _lift_in(node: exp.In) -> ir.InExpr:
    left = _lift_expr(node.this)
    sub = node.args.get("query")
    if sub is not None:
        rhs: tuple[ir.Expression, ...] | ir.Subquery = ir.Subquery(query=_lift_query(sub))
    else:
        opts = node.args.get("expressions") or []
        rhs = tuple(_lift_expr(e) for e in opts)
    return ir.InExpr(left=left, rhs=rhs, negate=False)


def _lift_case(node: exp.Case) -> ir.CaseExpr:
    branches: list[tuple[ir.Expression, ir.Expression]] = []
    for w in node.args.get("ifs") or []:
        if not isinstance(w, exp.If):
            raise UnsupportedSQLError(f"_lift_case branch: {type(w).__name__}")
        cond = _lift_expr(w.this)
        val = _lift_expr(w.args["true"])
        branches.append((cond, val))
    default = node.args.get("default")
    else_ = _lift_expr(default) if default is not None else None
    return ir.CaseExpr(branches=tuple(branches), else_=else_)


def _lift_aggregate(name: str, node: exp.Expression) -> ir.Aggregate:
    distinct = False
    inner = node.this
    if isinstance(inner, exp.Distinct):
        distinct = True
        if len(inner.expressions) == 1:
            args = (_lift_expr(inner.expressions[0]),)
        else:
            args = tuple(_lift_expr(e) for e in inner.expressions)
    elif inner is None:
        args = ()
    else:
        args = (_lift_expr(inner),)
    return ir.Aggregate(name=name.upper(), args=args, distinct=distinct)


def _lift_function(node: exp.Expression) -> ir.FunctionCall:
    name = node.name.upper() if node.name else type(node).__name__.upper()
    args: list[ir.Expression] = []
    for child in node.args.values():
        if child is None:
            continue
        if isinstance(child, list):
            for c in child:
                args.append(_lift_expr(c))
        elif isinstance(child, exp.Expression):
            args.append(_lift_expr(child))
    return ir.FunctionCall(name=name, args=tuple(args))
