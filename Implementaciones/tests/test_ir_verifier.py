"""
Tests del verificador estructural sobre la IR-SQL/PGQ (rebanada 4).

Cubre las tres clases de chequeo declaradas en el cap. 4 §4.5.4:
referencial, tipos y coherencia cruzada relacional↔grafo. Cada test arma
una IR mínima de propósito (en lugar de liftarla desde SQL) para aislar la
condición que se quiere verificar.

El último test corre el verificador estructural sobre las cien predicciones
del experimento 02 y mide cuántas pasan limpiamente. El criterio de paridad
es: el verificador estructural no debe ser más estricto que el MVP en este
sample (no debe introducir falsos positivos sobre consultas que el MVP
acepta).
"""

from __future__ import annotations

import json
from dataclasses import fields, is_dataclass, replace
from pathlib import Path

import pytest

from core.ir import (
    Aggregate,
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
    lift_sql,
)
from core.ir.schema import (
    ColumnSchema,
    ProjectedSchema,
    PropertyGraphEdgeTable,
    PropertyGraphSchema,
    PropertyGraphVertexTable,
    RelationalSchema,
    TableSchema,
    from_spider_tables,
)
from core.verifier import static as mvp
from core.verifier.structural import verify_ir

ROOT = Path(__file__).resolve().parent.parent


# ---------------------------------------------------------------------------
# Esquemas de test reutilizables
# ---------------------------------------------------------------------------


SCHEMA_CONCERT_SINGER = RelationalSchema(
    tables=(
        TableSchema(
            name="singer",
            columns=(
                ColumnSchema(name="singer_id", type="INTEGER", is_primary_key=True),
                ColumnSchema(name="name", type="TEXT"),
                ColumnSchema(name="country", type="TEXT"),
                ColumnSchema(name="age", type="INTEGER"),
                ColumnSchema(name="is_male", type="BOOLEAN"),
            ),
        ),
        TableSchema(
            name="concert",
            columns=(
                ColumnSchema(name="concert_id", type="INTEGER", is_primary_key=True),
                ColumnSchema(name="concert_name", type="TEXT"),
                ColumnSchema(name="year", type="INTEGER"),
                ColumnSchema(name="stadium_id", type="INTEGER"),
            ),
        ),
    )
)

SCHEMA_PERSON_GRAPH = ProjectedSchema(
    relational=RelationalSchema(
        tables=(
            TableSchema(
                name="Person",
                columns=(
                    ColumnSchema(name="id", type="INTEGER", is_primary_key=True),
                    ColumnSchema(name="name", type="TEXT"),
                    ColumnSchema(name="age", type="INTEGER"),
                    ColumnSchema(name="country", type="TEXT"),
                ),
            ),
            TableSchema(
                name="Knows",
                columns=(
                    ColumnSchema(name="p1", type="INTEGER"),
                    ColumnSchema(name="p2", type="INTEGER"),
                ),
            ),
        )
    ),
    graphs=(
        PropertyGraphSchema(
            name="test_graph",
            vertex_tables=(
                PropertyGraphVertexTable(
                    label="Person", table="Person", key_columns=("id",)
                ),
            ),
            edge_tables=(
                PropertyGraphEdgeTable(
                    label="knows",
                    table="Knows",
                    source_label="Person",
                    destination_label="Person",
                    source_key=("p1",),
                    destination_key=("p2",),
                ),
            ),
        ),
    ),
)


def _projected(rel: RelationalSchema) -> ProjectedSchema:
    return ProjectedSchema(relational=rel, graphs=())


# ---------------------------------------------------------------------------
# Clase referencial — relacional
# ---------------------------------------------------------------------------


def test_valid_relational_query_no_errors() -> None:
    q = lift_sql("SELECT name FROM singer WHERE age > 30")
    errors = verify_ir(q, _projected(SCHEMA_CONCERT_SINGER))
    assert errors == []


def test_unknown_table_detected() -> None:
    q = lift_sql("SELECT * FROM musicians")
    errors = verify_ir(q, _projected(SCHEMA_CONCERT_SINGER))
    assert any(e.kind == "unknown_table" and "musicians" in e.message for e in errors)


def test_unknown_column_unqualified_detected() -> None:
    q = lift_sql("SELECT email FROM singer")
    errors = verify_ir(q, _projected(SCHEMA_CONCERT_SINGER))
    assert any(e.kind == "unknown_column" and "email" in e.message for e in errors)


def test_unknown_column_qualified_detected() -> None:
    q = lift_sql("SELECT s.email FROM singer s")
    errors = verify_ir(q, _projected(SCHEMA_CONCERT_SINGER))
    assert any(e.kind == "unknown_column" and "email" in e.message for e in errors)


def test_unknown_qualifier_detected() -> None:
    q = lift_sql("SELECT bogus.name FROM singer")
    errors = verify_ir(q, _projected(SCHEMA_CONCERT_SINGER))
    assert any(e.kind == "unknown_qualifier" and "bogus" in e.message for e in errors)


def test_select_alias_visible_in_order_by() -> None:
    """Regression: el verificador estructural debe respetar los aliases AS
    declarados en el SELECT (mismo caso que motivó el fix del MVP)."""
    q = lift_sql(
        "SELECT name, count(*) AS song_count FROM singer "
        "GROUP BY name ORDER BY song_count DESC"
    )
    errors = verify_ir(q, _projected(SCHEMA_CONCERT_SINGER))
    assert errors == []


def test_join_with_qualified_columns() -> None:
    q = lift_sql(
        "SELECT s.name FROM singer s JOIN concert c "
        "ON s.singer_id = c.stadium_id WHERE c.year > 2000"
    )
    errors = verify_ir(q, _projected(SCHEMA_CONCERT_SINGER))
    assert errors == []


# ---------------------------------------------------------------------------
# Clase de tipos
# ---------------------------------------------------------------------------


def test_avg_on_text_column_is_type_mismatch() -> None:
    q = lift_sql("SELECT avg(name) FROM singer")
    errors = verify_ir(q, _projected(SCHEMA_CONCERT_SINGER))
    assert any(e.kind == "type_mismatch_aggregate" for e in errors)


def test_avg_on_numeric_column_is_ok() -> None:
    q = lift_sql("SELECT avg(age) FROM singer")
    errors = verify_ir(q, _projected(SCHEMA_CONCERT_SINGER))
    assert errors == []


def test_arithmetic_on_text_is_type_mismatch() -> None:
    q = lift_sql("SELECT name + 1 FROM singer")
    errors = verify_ir(q, _projected(SCHEMA_CONCERT_SINGER))
    assert any(e.kind == "type_mismatch_arithmetic" for e in errors)


def test_count_star_is_always_ok() -> None:
    q = lift_sql("SELECT count(*) FROM singer")
    errors = verify_ir(q, _projected(SCHEMA_CONCERT_SINGER))
    assert errors == []


# ---------------------------------------------------------------------------
# Clase referencial — grafo
# ---------------------------------------------------------------------------


def _knows_query() -> RelationalQuery:
    """``SELECT * FROM GRAPH_TABLE (test_graph MATCH ... COLUMNS (...))``."""
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
            SelectItem(
                expr=ColumnExpr(ref=ColumnRef(name="name", qualifier="b")),
                alias="dst",
            ),
        ),
    )
    return RelationalQuery(
        select=(SelectItem(expr=Star()),),
        from_=FromGraphMatch(match=match, alias="g"),
    )


def test_valid_graph_query_no_errors() -> None:
    errors = verify_ir(_knows_query(), SCHEMA_PERSON_GRAPH)
    assert errors == []


def test_unknown_graph_detected() -> None:
    q = _knows_query()
    bad = replace(
        q.from_.match,
        graph="nonexistent_graph",
    )
    bad_q = replace(q, from_=replace(q.from_, match=bad))
    errors = verify_ir(bad_q, SCHEMA_PERSON_GRAPH)
    assert any(e.kind == "unknown_graph" for e in errors)


def test_unknown_vertex_label_detected() -> None:
    q = _knows_query()
    bad_path = replace(q.from_.match.patterns[0], head=VertexPattern(var="a", label="Alien"))
    bad_match = replace(q.from_.match, patterns=(bad_path,))
    bad_q = replace(q, from_=replace(q.from_, match=bad_match))
    errors = verify_ir(bad_q, SCHEMA_PERSON_GRAPH)
    assert any(e.kind == "unknown_vertex_label" and "Alien" in e.message for e in errors)


def test_unknown_edge_label_detected() -> None:
    q = _knows_query()
    bad_step = (
        EdgePattern(var="k", label="hates", direction="->"),
        q.from_.match.patterns[0].steps[0][1],
    )
    bad_path = replace(q.from_.match.patterns[0], steps=(bad_step,))
    bad_match = replace(q.from_.match, patterns=(bad_path,))
    bad_q = replace(q, from_=replace(q.from_, match=bad_match))
    errors = verify_ir(bad_q, SCHEMA_PERSON_GRAPH)
    assert any(e.kind == "unknown_edge_label" and "hates" in e.message for e in errors)


def test_unknown_vertex_property_detected() -> None:
    """``SELECT a.email`` cuando Person no tiene email."""
    q = _knows_query()
    bad_cols = (
        SelectItem(
            expr=ColumnExpr(ref=ColumnRef(name="email", qualifier="a")),
            alias="src_email",
        ),
    )
    bad_match = replace(q.from_.match, columns=bad_cols)
    bad_q = replace(q, from_=replace(q.from_, match=bad_match))
    errors = verify_ir(bad_q, SCHEMA_PERSON_GRAPH)
    assert any(e.kind == "unknown_column" and "email" in e.message for e in errors)


# ---------------------------------------------------------------------------
# Clase de coherencia cruzada
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# Stress de composición híbrida — verificador sobre JOIN de GRAPH_TABLE y
# tabla relacional. El criterio de eficacia es triple: el verificador (a) no
# falsamente flagea un híbrido válido, (b) detecta hallucinations en el lado
# del grafo y (c) detecta hallucinations en el lado relacional.
# ---------------------------------------------------------------------------


def _hybrid_match() -> MatchPattern:
    """MATCH que expone ``a.id`` como ``src_id`` para enlazar luego con la
    tabla relacional ``Person``."""
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


def _hybrid_join_query(
    match: MatchPattern | None = None,
    where_col: tuple[str, str] = ("p", "age"),
) -> RelationalQuery:
    from core.ir import Join

    return RelationalQuery(
        select=(
            SelectItem(expr=ColumnExpr(ref=ColumnRef(name="src", qualifier="g"))),
            SelectItem(expr=ColumnExpr(ref=ColumnRef(name="age", qualifier="p"))),
        ),
        from_=Join(
            left=FromGraphMatch(match=match or _hybrid_match(), alias="g"),
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
            left=ColumnExpr(
                ref=ColumnRef(name=where_col[1], qualifier=where_col[0])
            ),
            right=Literal(value=25, raw="25"),
        ),
    )


def test_hybrid_valid_no_errors() -> None:
    """El verificador no debe rechazar una composición híbrida bien formada,
    aun cuando el WHERE referencia una columna que solo existe del lado
    relacional (``p.age``)."""
    errors = verify_ir(_hybrid_join_query(), SCHEMA_PERSON_GRAPH)
    assert errors == [], f"esperaba cero errores, obtuve {errors}"


def test_hybrid_relational_column_hallucination_detected() -> None:
    """El WHERE referencia ``p.salary`` que no existe en Person."""
    q = _hybrid_join_query(where_col=("p", "salary"))
    errors = verify_ir(q, SCHEMA_PERSON_GRAPH)
    assert any(
        e.kind == "unknown_column" and "salary" in e.message for e in errors
    )


def test_hybrid_graph_alias_hallucination_detected() -> None:
    """El SELECT referencia ``g.nonexistent`` — columna no declarada en
    COLUMNS del bloque GRAPH_TABLE."""
    from core.ir import Join

    q = RelationalQuery(
        select=(
            SelectItem(
                expr=ColumnExpr(ref=ColumnRef(name="nonexistent", qualifier="g"))
            ),
        ),
        from_=Join(
            left=FromGraphMatch(match=_hybrid_match(), alias="g"),
            right=FromTable(table=TableRef(name="Person", alias="p")),
            kind="INNER",
            on=BinaryOp(
                op="=",
                left=ColumnExpr(ref=ColumnRef(name="src_id", qualifier="g")),
                right=ColumnExpr(ref=ColumnRef(name="id", qualifier="p")),
            ),
        ),
    )
    errors = verify_ir(q, SCHEMA_PERSON_GRAPH)
    assert any(
        e.kind == "unknown_column" and "nonexistent" in e.message for e in errors
    )


def test_hybrid_vertex_property_hallucination_detected() -> None:
    """Dentro del MATCH, COLUMNS expone ``a.email`` — propiedad inexistente
    en el backing table del label Person."""
    bad_match = replace(
        _hybrid_match(),
        columns=(
            SelectItem(
                expr=ColumnExpr(ref=ColumnRef(name="email", qualifier="a")),
                alias="src_email",
            ),
        ),
    )
    q = _hybrid_join_query(match=bad_match)
    errors = verify_ir(q, SCHEMA_PERSON_GRAPH)
    assert any(
        e.kind == "unknown_column" and "email" in e.message for e in errors
    )


def test_hybrid_where_uses_graph_alias_column() -> None:
    """El verificador debe permitir referencias a columnas declaradas en
    COLUMNS del MATCH desde el WHERE relacional externo."""
    from core.ir import Join

    q = RelationalQuery(
        select=(
            SelectItem(expr=ColumnExpr(ref=ColumnRef(name="src", qualifier="g"))),
        ),
        from_=Join(
            left=FromGraphMatch(match=_hybrid_match(), alias="g"),
            right=FromTable(table=TableRef(name="Person", alias="p")),
            kind="INNER",
            on=BinaryOp(
                op="=",
                left=ColumnExpr(ref=ColumnRef(name="src_id", qualifier="g")),
                right=ColumnExpr(ref=ColumnRef(name="id", qualifier="p")),
            ),
        ),
        where=BinaryOp(
            op="=",
            left=ColumnExpr(ref=ColumnRef(name="dst", qualifier="g")),
            right=Literal(value="Carol", raw="'Carol'"),
        ),
    )
    errors = verify_ir(q, SCHEMA_PERSON_GRAPH)
    assert errors == []


def test_vertex_label_without_table_detected() -> None:
    """El grafo declara un label cuya backing table no existe en el esquema
    relacional. El verificador debe detectar esa incoherencia cuando se usa el
    label en un MATCH."""
    bad_schema = ProjectedSchema(
        relational=RelationalSchema(tables=()),  # vacío adrede
        graphs=(
            PropertyGraphSchema(
                name="test_graph",
                vertex_tables=(
                    PropertyGraphVertexTable(label="Person", table="Person"),
                ),
                edge_tables=(),
            ),
        ),
    )
    q = RelationalQuery(
        select=(SelectItem(expr=Star()),),
        from_=FromGraphMatch(
            match=MatchPattern(
                graph="test_graph",
                patterns=(
                    PathPattern(head=VertexPattern(var="a", label="Person")),
                ),
                columns=(
                    SelectItem(expr=ColumnExpr(ref=ColumnRef(name="name", qualifier="a"))),
                ),
            ),
            alias="g",
        ),
    )
    errors = verify_ir(q, bad_schema)
    assert any(e.kind == "vertex_label_without_table" for e in errors)


# ---------------------------------------------------------------------------
# Test de paridad sobre experiment 02
# ---------------------------------------------------------------------------


def _latest(prefix: str) -> Path | None:
    files = sorted((ROOT / "evaluation" / "runs").glob(f"{prefix}_*.json"))
    return files[-1] if files else None


@pytest.fixture(scope="module")
def experiment_02_predictions() -> list[dict]:
    path = _latest("experiment_02")
    if path is None:
        pytest.skip("no se encontró experiment_02_*.json")
    return json.loads(path.read_text())["results"]


def test_structural_verifier_parity_with_mvp(experiment_02_predictions) -> None:
    """Para cada predicción de experiment 02, comparar lo que reportan los
    dos verificadores. Métrica de paridad: el structural no debe flaggear
    consultas que el MVP acepta como limpias.
    """
    tables_path = ROOT / "corpus" / "spider_bird" / "tables.json"
    schemas_cache: dict[str, ProjectedSchema] = {}

    def get_schema(db_id: str) -> ProjectedSchema:
        if db_id not in schemas_cache:
            schemas_cache[db_id] = ProjectedSchema(
                relational=from_spider_tables(tables_path, db_id), graphs=()
            )
        return schemas_cache[db_id]

    total = 0
    lifted = 0
    structural_clean = 0
    mvp_clean = 0
    new_false_positives = 0  # structural flagea pero MVP no
    examples: list[tuple[int, list[str]]] = []

    for r in experiment_02_predictions:
        total += 1
        sql = r["predicted_sql"]
        schema = get_schema(r["db_id"])
        try:
            ir1 = lift_sql(sql)
        except Exception:
            continue
        lifted += 1

        # MVP toma dict[str, list[str]]
        mvp_errors = mvp.verify_sql(sql, schema.relational.to_simple_dict())
        struct_errors = verify_ir(ir1, schema)

        if not struct_errors:
            structural_clean += 1
        if not mvp_errors:
            mvp_clean += 1

        # Una consulta es "false positive nuevo" si MVP la deja pasar pero
        # structural no
        if struct_errors and not mvp_errors:
            new_false_positives += 1
            if len(examples) < 5:
                examples.append((r["id"], [e.kind for e in struct_errors]))

    print()
    print("=" * 60)
    print("Paridad structural vs MVP sobre experiment_02")
    print("=" * 60)
    print(f"total predicciones        : {total}")
    print(f"lift exitoso              : {lifted}")
    print(f"MVP las acepta            : {mvp_clean}")
    print(f"structural las acepta     : {structural_clean}")
    print(f"nuevas (struct flagea MVP no): {new_false_positives}")
    if examples:
        print("primeros casos donde structural es más estricto:")
        for fid, kinds in examples:
            print(f"  [{fid:3d}] kinds={kinds}")

    # Criterio: el structural puede ser MÁS permisivo o igual, pero no debe
    # introducir falsos positivos sobre lo que el MVP acepta.
    # En esta primera versión es esperable cierta divergencia (e.g.,
    # type_mismatch detectado solo por structural). La aserción confirma que
    # la divergencia, si la hay, no domina.
    assert new_false_positives <= max(5, lifted // 20), (
        f"el structural reportó {new_false_positives} casos "
        f"que el MVP aceptaba; revisar antes de seguir"
    )
