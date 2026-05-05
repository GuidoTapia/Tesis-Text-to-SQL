"""
Tests del orquestador del bucle de retroalimentación.

Los tests usan callables mockeados para ``emit_ir`` y ``execute`` —
así el bucle queda probado sin necesidad de una llamada real al LLM ni
conexión a un motor de base de datos. Los escenarios cubren los caminos
canónicos: éxito en el primer intento, recuperación tras un error
estático, recuperación tras un error de ejecución, y agotamiento de
iteraciones.
"""

from __future__ import annotations

from typing import Optional

import pytest

from core.feedback import (
    FeedbackDescriptor,
    FeedbackKind,
    answer_with_feedback,
    render_descriptors_for_prompt,
)
from core.ir import (
    Aggregate,
    BinaryOp,
    ColumnExpr,
    ColumnRef,
    FromTable,
    Literal,
    RelationalQuery,
    SelectItem,
    Star,
    TableRef,
    to_dict,
)
from core.ir.schema import (
    ColumnSchema,
    ProjectedSchema,
    RelationalSchema,
    TableSchema,
)


# ---------------------------------------------------------------------------
# Esquema y queries de fixture
# ---------------------------------------------------------------------------


SCHEMA = ProjectedSchema(
    relational=RelationalSchema(
        tables=(
            TableSchema(
                name="singer",
                columns=(
                    ColumnSchema(name="singer_id", type="INTEGER", is_primary_key=True),
                    ColumnSchema(name="name", type="TEXT"),
                    ColumnSchema(name="age", type="INTEGER"),
                ),
            ),
        )
    )
)


def _valid_query_count_singer() -> dict:
    """`SELECT count(*) FROM singer` como payload IR."""
    q = RelationalQuery(
        select=(SelectItem(expr=Aggregate(name="COUNT", args=(Star(),))),),
        from_=FromTable(table=TableRef(name="singer")),
    )
    return to_dict(q)


def _query_with_unknown_column() -> dict:
    """``SELECT email FROM singer`` — `email` no existe."""
    q = RelationalQuery(
        select=(SelectItem(expr=ColumnExpr(ref=ColumnRef(name="email"))),),
        from_=FromTable(table=TableRef(name="singer")),
    )
    return to_dict(q)


def _query_with_avg_on_text() -> dict:
    """``SELECT avg(name) FROM singer`` — type mismatch en agregación."""
    q = RelationalQuery(
        select=(
            SelectItem(
                expr=Aggregate(name="AVG", args=(ColumnExpr(ref=ColumnRef(name="name")),))
            ),
        ),
        from_=FromTable(table=TableRef(name="singer")),
    )
    return to_dict(q)


# ---------------------------------------------------------------------------
# Helpers para construir mocks
# ---------------------------------------------------------------------------


def _scripted_emit_ir(payloads: list[dict]):
    """Devuelve un emit_ir que retorna los payloads en orden."""
    iteration_state = {"i": 0}

    def emit_ir(question, schema, descriptors):
        i = iteration_state["i"]
        iteration_state["i"] = i + 1
        if i < len(payloads):
            return payloads[i]
        # Si se piden más payloads de los previstos, repetir el último
        return payloads[-1]

    return emit_ir


def _execute_succeeds(sql: str) -> tuple[bool, Optional[str], Optional[list]]:
    return True, None, [(7,)]  # Simulamos 7 cantantes


def _execute_fails_with(error_msg: str):
    def execute(sql: str) -> tuple[bool, Optional[str], Optional[list]]:
        return False, error_msg, None

    return execute


# ---------------------------------------------------------------------------
# Casos canónicos
# ---------------------------------------------------------------------------


def test_success_on_first_attempt() -> None:
    payloads = [_valid_query_count_singer()]
    result = answer_with_feedback(
        question="cuántos cantantes hay",
        schema=SCHEMA,
        emit_ir=_scripted_emit_ir(payloads),
        execute=_execute_succeeds,
    )
    assert result.success
    assert result.n_iterations == 1
    assert result.final_rows == [(7,)]
    assert result.attempts[0].failed_stage is None


def test_recovery_after_verifier_error() -> None:
    """Primer intento alucina columna; verifier la atrapa; segundo intento
    es válido y la ejecución devuelve filas."""
    payloads = [
        _query_with_unknown_column(),  # alucina 'email'
        _valid_query_count_singer(),  # arregla en el segundo intento
    ]
    result = answer_with_feedback(
        question="cuántos cantantes hay",
        schema=SCHEMA,
        emit_ir=_scripted_emit_ir(payloads),
        execute=_execute_succeeds,
    )
    assert result.success
    assert result.n_iterations == 2
    # Primer intento debe haber fallado en verifier con unknown_column
    first = result.attempts[0]
    assert first.failed_stage == "verifier"
    assert any(d.kind == FeedbackKind.UNKNOWN_COLUMN for d in first.descriptors)
    # Segundo intento exitoso
    second = result.attempts[1]
    assert second.failed_stage is None
    assert second.compiled_sql is not None


def test_recovery_after_execution_error() -> None:
    """Verifier acepta el primer payload; el motor falla; en el segundo
    intento el modelo emite algo distinto que ejecuta."""
    state = {"i": 0}

    def execute(sql: str):
        i = state["i"]
        state["i"] = i + 1
        if i == 0:
            return False, "Conversion Error: invalid date field format: \"\"", None
        return True, None, [(42,)]

    payloads = [
        _valid_query_count_singer(),
        _valid_query_count_singer(),
    ]
    result = answer_with_feedback(
        question="x", schema=SCHEMA,
        emit_ir=_scripted_emit_ir(payloads),
        execute=execute,
    )
    assert result.success
    assert result.n_iterations == 2
    first = result.attempts[0]
    assert first.failed_stage == "execution"
    assert first.descriptors[0].kind == FeedbackKind.DATA_CONVERSION_FAILURE
    second = result.attempts[1]
    assert second.failed_stage is None
    assert second.rows == [(42,)]


def test_failure_after_max_iterations() -> None:
    """Si todos los intentos fallan, success=False y attempts contiene todo."""
    payloads = [_query_with_unknown_column()] * 3  # siempre alucina
    result = answer_with_feedback(
        question="x", schema=SCHEMA,
        emit_ir=_scripted_emit_ir(payloads),
        execute=_execute_succeeds,
        max_iterations=3,
    )
    assert not result.success
    assert result.n_iterations == 3
    assert all(a.failed_stage == "verifier" for a in result.attempts)


def test_descriptors_accumulate_across_iterations() -> None:
    """El emit_ir debe recibir todos los descriptores acumulados, no solo
    los del último intento."""
    seen_lengths: list[int] = []

    def emit_ir(question, schema, descriptors):
        seen_lengths.append(len(descriptors))
        # Iteración 0: primer intento, sin descriptores
        # Iteración 1: ya hay un descriptor (unknown_column del intento 0)
        # Iteración 2: dos descriptores
        return _query_with_unknown_column()  # siempre falla

    answer_with_feedback(
        question="x", schema=SCHEMA, emit_ir=emit_ir,
        execute=_execute_succeeds, max_iterations=3,
    )
    assert seen_lengths == [0, 1, 2]


def test_parser_failure_yields_syntax_descriptor() -> None:
    """Un payload malformado no parseable debe producir descriptor de
    syntax_error y permitir reintentar."""
    bad_payloads = [
        {"type": "Frankenstein"},  # tipo desconocido
        _valid_query_count_singer(),
    ]
    result = answer_with_feedback(
        question="x", schema=SCHEMA,
        emit_ir=_scripted_emit_ir(bad_payloads),
        execute=_execute_succeeds,
    )
    assert result.success
    assert result.attempts[0].failed_stage == "parser"
    assert result.attempts[0].descriptors[0].kind == FeedbackKind.SYNTAX_ERROR


def test_render_descriptors_for_prompt_format() -> None:
    descriptors = (
        FeedbackDescriptor.from_kind(FeedbackKind.UNKNOWN_COLUMN, stage="verifier"),
        FeedbackDescriptor.from_kind(
            FeedbackKind.GROUP_BY_VIOLATION, stage="execution"
        ),
    )
    text = render_descriptors_for_prompt(descriptors)
    assert "Intentos anteriores" in text
    assert "verifier" in text
    assert "execution" in text
    assert "unknown_column" in text
    assert "group_by_violation" in text
    # No debe incluir raw_excerpt — política de retroalimentación estructurada
    # (los descriptores en este test no tienen raw_excerpt, pero la función
    # no debe leerlo)


def test_render_descriptors_empty_returns_empty_string() -> None:
    assert render_descriptors_for_prompt(()) == ""
