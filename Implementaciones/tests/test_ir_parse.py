"""
Tests del parser/serializer JSON↔IR (rebanada 5).

Cubre:

- Round-trip ``parse_ir(to_dict(x)) == x`` sobre instancias canónicas de IR
  (relacionales, de grafo, híbridas).
- Round-trip a partir del lifter sqlglot: ``lift → to_dict → parse → ==``.
- Casos de error: campo ``type`` ausente, tipo desconocido, fields inválidos.
- Estabilidad: el formato JSON no incluye ``None`` ni listas que el parser
  no pueda reconstruir.
"""

from __future__ import annotations

import json

import pytest

from core.ir import (
    Aggregate,
    BinaryOp,
    ColumnExpr,
    ColumnRef,
    EdgePattern,
    FromGraphMatch,
    FromTable,
    IRParseError,
    Literal,
    MatchPattern,
    OrderItem,
    PathPattern,
    RelationalQuery,
    SelectItem,
    Star,
    TableRef,
    VertexPattern,
    lift_sql,
    parse_ir,
    to_dict,
)


# ---------------------------------------------------------------------------
# Round-trip sobre instancias armadas a mano
# ---------------------------------------------------------------------------


def _assert_roundtrip(node) -> None:
    serialized = to_dict(node)
    # Asegurar que sobrevive a un serialize/deserialize JSON real
    json_str = json.dumps(serialized)
    parsed_dict = json.loads(json_str)
    parsed = parse_ir(parsed_dict)
    assert parsed == node, (
        f"\noriginal: {node}\n"
        f"serialized: {serialized}\n"
        f"parsed: {parsed}"
    )


def test_roundtrip_simple_count() -> None:
    q = RelationalQuery(
        select=(SelectItem(expr=Aggregate(name="COUNT", args=(Star(),))),),
        from_=FromTable(table=TableRef(name="singer")),
    )
    _assert_roundtrip(q)


def test_roundtrip_filter_and_order() -> None:
    q = RelationalQuery(
        select=(SelectItem(expr=ColumnExpr(ref=ColumnRef(name="name"))),),
        from_=FromTable(table=TableRef(name="singer")),
        where=BinaryOp(
            op=">",
            left=ColumnExpr(ref=ColumnRef(name="age")),
            right=Literal(value=30),
        ),
        order_by=(
            OrderItem(
                expr=ColumnExpr(ref=ColumnRef(name="age")),
                direction="DESC",
            ),
        ),
        limit=5,
    )
    _assert_roundtrip(q)


def test_roundtrip_pure_graph() -> None:
    match = MatchPattern(
        graph="test_graph",
        patterns=(
            PathPattern(
                head=VertexPattern(var="a", label="Person"),
                steps=(
                    (
                        EdgePattern(var="k", label="knows", direction="->"),
                        VertexPattern(var="b", label="Person"),
                    ),
                ),
            ),
        ),
        columns=(
            SelectItem(
                expr=ColumnExpr(ref=ColumnRef(name="name", qualifier="a")),
                alias="src",
            ),
        ),
    )
    q = RelationalQuery(
        select=(SelectItem(expr=Star()),),
        from_=FromGraphMatch(match=match, alias="g"),
    )
    _assert_roundtrip(q)


def test_roundtrip_hybrid_join() -> None:
    """Cobertura del caso híbrido canónico — JOIN entre GRAPH_TABLE y tabla
    relacional."""
    from core.ir import Join

    match = MatchPattern(
        graph="test_graph",
        patterns=(
            PathPattern(
                head=VertexPattern(var="a", label="Person"),
                steps=(
                    (
                        EdgePattern(var="k", label="knows", direction="->"),
                        VertexPattern(var="b", label="Person"),
                    ),
                ),
            ),
        ),
        columns=(
            SelectItem(
                expr=ColumnExpr(ref=ColumnRef(name="id", qualifier="a")),
                alias="src_id",
            ),
        ),
    )
    q = RelationalQuery(
        select=(SelectItem(expr=ColumnExpr(ref=ColumnRef(name="age", qualifier="p"))),),
        from_=Join(
            left=FromGraphMatch(match=match, alias="g"),
            right=FromTable(table=TableRef(name="Person", alias="p")),
            kind="INNER",
            on=BinaryOp(
                op="=",
                left=ColumnExpr(ref=ColumnRef(name="src_id", qualifier="g")),
                right=ColumnExpr(ref=ColumnRef(name="id", qualifier="p")),
            ),
        ),
    )
    _assert_roundtrip(q)


# ---------------------------------------------------------------------------
# Round-trip a partir del lifter sqlglot
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "sql",
    [
        "SELECT count(*) FROM singer",
        "SELECT name FROM singer WHERE age > 30",
        "SELECT DISTINCT country FROM singer",
        "SELECT name FROM singer ORDER BY age DESC LIMIT 5",
        "SELECT s.name FROM singer s JOIN concert c ON s.id = c.sid WHERE c.year > 2000",
        "SELECT country, count(*) FROM singer GROUP BY country HAVING count(*) > 1",
        "SELECT count(DISTINCT country) FROM singer",
        "SELECT name FROM singer WHERE country IN ('Italy', 'France')",
        "SELECT name FROM singer WHERE singer_id IN (SELECT singer_id FROM concert)",
        "SELECT CASE WHEN age < 18 THEN 'minor' ELSE 'adult' END FROM singer",
        "SELECT country FROM singer EXCEPT SELECT country FROM concert",
        "SELECT name FROM singer WHERE country IS NULL",
        "SELECT CAST(age AS REAL) FROM singer",
    ],
)
def test_roundtrip_via_lifter(sql: str) -> None:
    ir_node = lift_sql(sql)
    _assert_roundtrip(ir_node)


# ---------------------------------------------------------------------------
# Errores de parseo
# ---------------------------------------------------------------------------


def test_parse_missing_type_field() -> None:
    with pytest.raises(IRParseError, match="sin campo 'type'"):
        parse_ir({"name": "singer"})


def test_parse_unknown_type() -> None:
    with pytest.raises(IRParseError, match="tipo de nodo desconocido"):
        parse_ir({"type": "Frankenstein"})


def test_parse_invalid_field_for_type() -> None:
    with pytest.raises(IRParseError, match="campos inválidos para"):
        parse_ir({"type": "TableRef", "bogus_field": "x"})


def test_parse_unsupported_json_value() -> None:
    with pytest.raises(IRParseError, match="no soportado"):
        parse_ir(set())  # type: ignore[arg-type]


# ---------------------------------------------------------------------------
# Propiedad: el JSON producido es válido como JSON estricto
# ---------------------------------------------------------------------------


def test_serialized_form_is_strict_json() -> None:
    q = RelationalQuery(
        select=(
            SelectItem(
                expr=BinaryOp(
                    op="+",
                    left=Literal(value=1, raw="1"),
                    right=Literal(value=2, raw="2"),
                )
            ),
        ),
    )
    # No debe levantar
    serialized = to_dict(q)
    json_str = json.dumps(serialized)
    assert json_str.startswith("{")
    # Verificamos que el round-trip preserva la igualdad
    assert parse_ir(json.loads(json_str)) == q
