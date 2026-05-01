"""
Tests unitarios para los nodos de la IR, el lifter y el compiler.

Cubre los caminos canónicos: SELECT con FROM, JOIN, WHERE, GROUP BY, HAVING,
ORDER BY, LIMIT, agregaciones, funciones escalares, IN/NOT IN con literales y
subconsultas, EXISTS, CASE, CAST, set operations.
"""

from __future__ import annotations

from core.ir import (
    Aggregate,
    BinaryOp,
    ColumnExpr,
    ColumnRef,
    FromTable,
    Literal,
    OrderItem,
    RelationalQuery,
    SelectItem,
    Star,
    TableRef,
    compile_query,
    lift_sql,
)


def _strip_parens(node):
    """Elimina ParenExpr/parens cosméticos de la IR para comparar estructura."""
    from core.ir import nodes as ir
    from dataclasses import is_dataclass, fields, replace

    if isinstance(node, ir.ParenExpr):
        return _strip_parens(node.inner)
    if isinstance(node, tuple):
        return tuple(_strip_parens(x) for x in node)
    if is_dataclass(node):
        kwargs = {f.name: _strip_parens(getattr(node, f.name)) for f in fields(node)}
        return replace(node, **kwargs)
    return node


# ---------------------------------------------------------------------------
# Construcción manual y compilación
# ---------------------------------------------------------------------------


def test_compile_simple_count() -> None:
    q = RelationalQuery(
        select=(SelectItem(expr=Aggregate(name="COUNT", args=(Star(),))),),
        from_=FromTable(table=TableRef(name="singer")),
    )
    assert compile_query(q) == "SELECT COUNT(*) FROM singer"


def test_compile_filter_and_order() -> None:
    q = RelationalQuery(
        select=(
            SelectItem(expr=ColumnExpr(ref=ColumnRef(name="name"))),
        ),
        from_=FromTable(table=TableRef(name="singer")),
        where=BinaryOp(
            op=">",
            left=ColumnExpr(ref=ColumnRef(name="age")),
            right=Literal(value=30),
        ),
        order_by=(
            OrderItem(expr=ColumnExpr(ref=ColumnRef(name="age")), direction="DESC"),
        ),
        limit=5,
    )
    expected = "SELECT name FROM singer WHERE (age > 30) ORDER BY age DESC LIMIT 5"
    assert compile_query(q) == expected


# ---------------------------------------------------------------------------
# Round-trip: lift_sql ∘ compile_query es semánticamente la identidad
# ---------------------------------------------------------------------------


def _assert_equiv(sql: str) -> None:
    """Invariante de round-trip: lift → compile → lift produce la misma IR.

    Ignora ParenExpr cosméticos porque el compilador agrega paréntesis liberales
    para no depender de la precedencia del dialecto."""
    ir1 = _strip_parens(lift_sql(sql))
    sql2 = compile_query(ir1)
    ir2 = _strip_parens(lift_sql(sql2))
    assert ir1 == ir2, (
        f"\nentrada:    {sql}\nrecompilado: {sql2}\n"
        f"\nIR original: {ir1}\nIR re-lifted: {ir2}"
    )


def test_roundtrip_count_star() -> None:
    _assert_equiv("SELECT count(*) FROM singer")


def test_roundtrip_simple_filter() -> None:
    _assert_equiv("SELECT name FROM singer WHERE age > 30")


def test_roundtrip_distinct() -> None:
    _assert_equiv("SELECT DISTINCT country FROM singer")


def test_roundtrip_order_limit() -> None:
    _assert_equiv("SELECT name FROM singer ORDER BY age DESC LIMIT 5")


def test_roundtrip_join_and_where() -> None:
    _assert_equiv(
        "SELECT s.name, c.year FROM singer AS s JOIN concert AS c "
        "ON s.singer_id = c.singer_id WHERE c.year > 2000"
    )


def test_roundtrip_left_join() -> None:
    _assert_equiv(
        "SELECT s.name FROM singer AS s LEFT JOIN concert AS c "
        "ON s.singer_id = c.singer_id"
    )


def test_roundtrip_group_having() -> None:
    _assert_equiv(
        "SELECT country, count(*) FROM singer GROUP BY country HAVING count(*) > 1"
    )


def test_roundtrip_count_distinct() -> None:
    _assert_equiv("SELECT count(DISTINCT country) FROM singer")


def test_roundtrip_in_literals() -> None:
    _assert_equiv("SELECT name FROM singer WHERE country IN ('Italy', 'France')")


def test_roundtrip_in_subquery() -> None:
    _assert_equiv(
        "SELECT name FROM singer WHERE singer_id IN "
        "(SELECT singer_id FROM concert)"
    )


def test_roundtrip_not_in_subquery() -> None:
    _assert_equiv(
        "SELECT name FROM singer WHERE singer_id NOT IN "
        "(SELECT singer_id FROM concert)"
    )


def test_roundtrip_exists() -> None:
    _assert_equiv(
        "SELECT name FROM singer WHERE EXISTS "
        "(SELECT 1 FROM concert WHERE concert.singer_id = singer.singer_id)"
    )


def test_roundtrip_like() -> None:
    _assert_equiv("SELECT name FROM singer WHERE name LIKE 'A%'")


def test_roundtrip_between() -> None:
    _assert_equiv("SELECT name FROM singer WHERE age BETWEEN 18 AND 65")


def test_roundtrip_is_null() -> None:
    _assert_equiv("SELECT name FROM singer WHERE country IS NULL")


def test_roundtrip_is_not_null() -> None:
    _assert_equiv("SELECT name FROM singer WHERE country IS NOT NULL")


def test_roundtrip_case() -> None:
    _assert_equiv(
        "SELECT CASE WHEN age < 18 THEN 'minor' ELSE 'adult' END FROM singer"
    )


def test_roundtrip_cast() -> None:
    _assert_equiv("SELECT CAST(age AS REAL) FROM singer")


def test_roundtrip_subquery_in_from() -> None:
    _assert_equiv(
        "SELECT s.name FROM (SELECT * FROM singer) AS s WHERE s.age > 30"
    )


def test_roundtrip_set_op_except() -> None:
    _assert_equiv(
        "SELECT country FROM singer EXCEPT SELECT country FROM concert"
    )


def test_roundtrip_set_op_union() -> None:
    _assert_equiv(
        "SELECT country FROM singer UNION SELECT country FROM concert"
    )
