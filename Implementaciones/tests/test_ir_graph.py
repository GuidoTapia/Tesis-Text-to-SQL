"""
Tests del bloque de grafo de la IR-SQL/PGQ (rebanada 2).

Combina dos niveles de cobertura:

1. Tests *compile-only* sobre nodos construidos a mano. Validan la sintaxis
   exacta que el compilador emite para vértices, aristas, patrones de camino,
   bloques MATCH completos y composición híbrida con FROM relacional.

2. Un test *end-to-end* que crea un property graph mínimo en DuckDB,
   compila IR a SQL/PGQ y verifica que la consulta ejecuta y devuelve el
   resultado esperado. Es la prueba que valida la sintaxis contra el motor
   real, incluyendo restricciones operativas como la variable obligatoria de
   arista de DuckPGQ.
"""

from __future__ import annotations

import duckdb
import pytest

from core.ir import (
    BinaryOp,
    ColumnExpr,
    ColumnRef,
    EdgePattern,
    FromGraphMatch,
    FromTable,
    Literal,
    MatchPattern,
    PathPattern,
    RelationalQuery,
    SelectItem,
    Star,
    TableRef,
    VertexPattern,
    compile_query,
)
from core.ir.compile import (
    _compile_edge_pattern,
    _compile_match_pattern,
    _compile_path_pattern,
    _compile_vertex_pattern,
)


# ---------------------------------------------------------------------------
# Tests compile-only sobre los nodos atómicos
# ---------------------------------------------------------------------------


def test_compile_vertex_with_label() -> None:
    assert _compile_vertex_pattern(VertexPattern(var="a", label="Person")) == "(a:Person)"


def test_compile_vertex_without_label() -> None:
    assert _compile_vertex_pattern(VertexPattern(var="x")) == "(x)"


def test_compile_edge_directed_forward() -> None:
    e = EdgePattern(var="k", label="knows", direction="->")
    assert _compile_edge_pattern(e) == "-[k:knows]->"


def test_compile_edge_directed_backward() -> None:
    e = EdgePattern(var="k", label="knows", direction="<-")
    assert _compile_edge_pattern(e) == "<-[k:knows]-"


def test_compile_edge_undirected() -> None:
    e = EdgePattern(var="k", label="knows", direction="-")
    assert _compile_edge_pattern(e) == "-[k:knows]-"


def test_compile_path_two_hops() -> None:
    """Patrón ``(a:Person)-[k1:knows]->(b:Person)-[k2:knows]->(c:Person)``."""
    p = PathPattern(
        head=VertexPattern(var="a", label="Person"),
        steps=(
            (
                EdgePattern(var="k1", label="knows", direction="->"),
                VertexPattern(var="b", label="Person"),
            ),
            (
                EdgePattern(var="k2", label="knows", direction="->"),
                VertexPattern(var="c", label="Person"),
            ),
        ),
    )
    expected = "(a:Person)-[k1:knows]->(b:Person)-[k2:knows]->(c:Person)"
    assert _compile_path_pattern(p) == expected


# ---------------------------------------------------------------------------
# Tests compile sobre MatchPattern completo
# ---------------------------------------------------------------------------


def _knows_match(graph: str = "test_graph") -> MatchPattern:
    return MatchPattern(
        graph=graph,
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
            SelectItem(
                expr=ColumnExpr(ref=ColumnRef(name="name", qualifier="b")),
                alias="dst",
            ),
        ),
    )


def test_compile_match_pattern_basic() -> None:
    out = _compile_match_pattern(_knows_match())
    expected = (
        "test_graph MATCH (a:Person)-[k:knows]->(b:Person) "
        "COLUMNS (a.name AS src, b.name AS dst)"
    )
    assert out == expected


def test_compile_match_pattern_with_where() -> None:
    base = _knows_match()
    m = MatchPattern(
        graph=base.graph,
        patterns=base.patterns,
        where=BinaryOp(
            op="=",
            left=ColumnExpr(ref=ColumnRef(name="name", qualifier="a")),
            right=Literal(value="Alice", raw="'Alice'"),
        ),
        columns=base.columns,
    )
    out = _compile_match_pattern(m)
    assert "WHERE (a.name = 'Alice')" in out
    assert "COLUMNS (a.name AS src, b.name AS dst)" in out


# ---------------------------------------------------------------------------
# Tests compile sobre la consulta completa con FromGraphMatch
# ---------------------------------------------------------------------------


def test_compile_pure_graph_query() -> None:
    """``SELECT * FROM GRAPH_TABLE (...)`` — caso puramente de grafo."""
    q = RelationalQuery(
        select=(SelectItem(expr=Star()),),
        from_=FromGraphMatch(match=_knows_match(), alias="g"),
    )
    expected = (
        "SELECT * FROM GRAPH_TABLE (test_graph MATCH (a:Person)-[k:knows]->(b:Person) "
        "COLUMNS (a.name AS src, b.name AS dst)) g"
    )
    assert compile_query(q) == expected


def test_compile_hybrid_query_with_external_filter() -> None:
    """``SELECT g.src FROM GRAPH_TABLE (...) g WHERE g.dst = 'Carol'``."""
    q = RelationalQuery(
        select=(
            SelectItem(expr=ColumnExpr(ref=ColumnRef(name="src", qualifier="g"))),
        ),
        from_=FromGraphMatch(match=_knows_match(), alias="g"),
        where=BinaryOp(
            op="=",
            left=ColumnExpr(ref=ColumnRef(name="dst", qualifier="g")),
            right=Literal(value="Carol", raw="'Carol'"),
        ),
    )
    out = compile_query(q)
    assert "GRAPH_TABLE (" in out
    assert "WHERE (g.dst = 'Carol')" in out
    assert out.startswith("SELECT g.src FROM GRAPH_TABLE")


# ---------------------------------------------------------------------------
# Test end-to-end contra DuckDB real
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def graph_db() -> duckdb.DuckDBPyConnection:
    """Levanta un property graph mínimo en DuckDB para los tests de ejecución.

    El esquema incluye columnas de propiedad (age, country) en ``Person`` que
    no se exponen como vértices/aristas del grafo pero sí están disponibles
    para la composición híbrida (JOIN del resultado del MATCH con la tabla
    relacional).
    """
    con = duckdb.connect(":memory:")
    con.execute("INSTALL duckpgq FROM community")
    con.execute("LOAD duckpgq")
    con.execute(
        "CREATE TABLE Person (id INTEGER, name VARCHAR, age INTEGER, country VARCHAR)"
    )
    con.execute(
        "INSERT INTO Person VALUES "
        "(1, 'Alice', 30, 'AR'), (2, 'Bob', 22, 'BR'), (3, 'Carol', 45, 'AR')"
    )
    con.execute("CREATE TABLE Knows (p1 INTEGER, p2 INTEGER)")
    con.execute("INSERT INTO Knows VALUES (1, 2), (2, 3)")
    con.execute(
        """
        CREATE PROPERTY GRAPH test_graph
          VERTEX TABLES (Person)
          EDGE TABLES (
            Knows SOURCE KEY (p1) REFERENCES Person (id)
                  DESTINATION KEY (p2) REFERENCES Person (id)
                  LABEL knows
          )
        """
    )
    return con


def test_execute_pure_graph(graph_db) -> None:
    q = RelationalQuery(
        select=(SelectItem(expr=Star()),),
        from_=FromGraphMatch(match=_knows_match(), alias="g"),
    )
    sql = compile_query(q)
    rows = graph_db.execute(sql).fetchall()
    assert sorted(rows) == [("Alice", "Bob"), ("Bob", "Carol")]


def test_execute_match_with_internal_where(graph_db) -> None:
    base = _knows_match()
    m = MatchPattern(
        graph=base.graph,
        patterns=base.patterns,
        where=BinaryOp(
            op="=",
            left=ColumnExpr(ref=ColumnRef(name="name", qualifier="a")),
            right=Literal(value="Alice", raw="'Alice'"),
        ),
        columns=(
            SelectItem(
                expr=ColumnExpr(ref=ColumnRef(name="name", qualifier="b")),
                alias="friend",
            ),
        ),
    )
    q = RelationalQuery(
        select=(SelectItem(expr=Star()),),
        from_=FromGraphMatch(match=m, alias="g"),
    )
    sql = compile_query(q)
    rows = graph_db.execute(sql).fetchall()
    assert rows == [("Bob",)]


def test_execute_hybrid_external_filter(graph_db) -> None:
    """El resultado del MATCH se trata como tabla derivada y se filtra desde
    la cláusula relacional. Es el caso canónico de HybridComposition."""
    q = RelationalQuery(
        select=(
            SelectItem(expr=ColumnExpr(ref=ColumnRef(name="src", qualifier="g"))),
        ),
        from_=FromGraphMatch(match=_knows_match(), alias="g"),
        where=BinaryOp(
            op="=",
            left=ColumnExpr(ref=ColumnRef(name="dst", qualifier="g")),
            right=Literal(value="Carol", raw="'Carol'"),
        ),
    )
    sql = compile_query(q)
    rows = graph_db.execute(sql).fetchall()
    assert rows == [("Bob",)]


# ---------------------------------------------------------------------------
# Stress de composición híbrida — JOIN entre GRAPH_TABLE y tabla relacional
# ---------------------------------------------------------------------------


def _knows_match_with_ids() -> MatchPattern:
    """Variante de ``_knows_match`` que también expone los ids como columnas
    para permitir el JOIN con la tabla relacional ``Person``."""
    from core.ir import EdgePattern, PathPattern, VertexPattern

    return MatchPattern(
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
            SelectItem(
                expr=ColumnExpr(ref=ColumnRef(name="name", qualifier="b")),
                alias="dst",
            ),
            SelectItem(
                expr=ColumnExpr(ref=ColumnRef(name="id", qualifier="a")),
                alias="src_id",
            ),
        ),
    )


def _hybrid_join_query() -> RelationalQuery:
    """Caso canónico de HybridComposition: el resultado de un GRAPH_TABLE
    participa en un JOIN con una tabla relacional, y el WHERE filtra por una
    propiedad presente solo en la tabla relacional (``age``)."""
    from core.ir import Join

    return RelationalQuery(
        select=(
            SelectItem(expr=ColumnExpr(ref=ColumnRef(name="src", qualifier="g"))),
            SelectItem(expr=ColumnExpr(ref=ColumnRef(name="dst", qualifier="g"))),
            SelectItem(expr=ColumnExpr(ref=ColumnRef(name="age", qualifier="p"))),
            SelectItem(expr=ColumnExpr(ref=ColumnRef(name="country", qualifier="p"))),
        ),
        from_=Join(
            left=FromGraphMatch(match=_knows_match_with_ids(), alias="g"),
            right=FromTable(table=TableRef(name="Person", alias="p")),
            kind="INNER",
            on=BinaryOp(
                op="=",
                left=ColumnExpr(ref=ColumnRef(name="src_id", qualifier="g")),
                right=ColumnExpr(ref=ColumnRef(name="id", qualifier="p")),
            ),
        ),
        where=BinaryOp(
            op=">",
            left=ColumnExpr(ref=ColumnRef(name="age", qualifier="p")),
            right=Literal(value=25, raw="25"),
        ),
    )


def test_execute_hybrid_join_with_relational_table(graph_db) -> None:
    """El caso híbrido canónico: relacionar resultados del MATCH con datos
    de una tabla relacional vía JOIN, filtrando por columnas que solo
    existen en la tabla relacional."""
    sql = compile_query(_hybrid_join_query())
    rows = graph_db.execute(sql).fetchall()
    # Alice (id=1) tiene age=30 (>25) y conoce a Bob; Carol no conoce a nadie
    # como source. Bob (id=2) tiene age=22 (<25), filtrado.
    assert rows == [("Alice", "Bob", 30, "AR")]
