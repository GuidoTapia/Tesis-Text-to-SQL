"""
Tests del clasificador de errores de ejecución y de la conversión de
errores del verificador a descriptores.

Cubre los patrones que se observaron en los experimentos documentados
en lab_notebook (BINDER, Conversion, encoding UTF-8, restricciones de
DuckPGQ, etc.). Los mensajes de error son los reales recolectados en
los JSON de evaluation/runs.
"""

from __future__ import annotations

import pytest

from core.feedback import (
    FeedbackDescriptor,
    FeedbackKind,
    classify_execution_error,
    descriptor_from_execution_error,
    descriptors_from_verifier_errors,
)
from core.verifier.errors import VerificationError


# ---------------------------------------------------------------------------
# Mensajes recolectados de los experimentos reales
# ---------------------------------------------------------------------------


_OBSERVED: list[tuple[str, FeedbackKind]] = [
    # Exp 02: BINDER GROUP BY
    (
        "Binder Error: column CountryCode must appear in the GROUP BY "
        "clause or be used in an aggregate function",
        FeedbackKind.GROUP_BY_VIOLATION,
    ),
    (
        'Binder Error: column "winner_name" must appear in the GROUP BY '
        "clause or must be part of an aggregate function.",
        FeedbackKind.GROUP_BY_VIOLATION,
    ),
    # Exp 02: BINDER tipos
    (
        "Binder Error: Cannot compare values of type BIGINT and VARCHAR "
        "in IN/ANY/ALL clause - an explicit cast is required",
        FeedbackKind.TYPE_INCOMPATIBLE,
    ),
    # Exp 02: Conversion errors
    (
        "Conversion Error: Could not convert string 'null' to FLOAT when "
        "casting from source column Horsepower",
        FeedbackKind.DATA_CONVERSION_FAILURE,
    ),
    (
        'Conversion Error: invalid date field format: "", expected format '
        "is (YYYY-MM-DD)",
        FeedbackKind.DATA_CONVERSION_FAILURE,
    ),
    # Exp 02: Mismatch type por sqlite/duckdb
    (
        'Mismatch Type Error: Invalid type in column "player_id": column '
        "was declared as integer, found \"\" of type \"text\" instead.",
        FeedbackKind.DATA_CONVERSION_FAILURE,
    ),
    # Exp 03: encoding
    (
        "Could not decode to UTF-8 column 'last_name' with text "
        "'Treyes Albarrac\\xeeN'",
        FeedbackKind.DATA_ENCODING_FAILURE,
    ),
    # Exp 04: prosa del LLM como SQL
    ('near "I": syntax error', FeedbackKind.SYNTAX_ERROR),
    # Exp 04: alucinación atrapada por sqlite
    ("no such column: Color", FeedbackKind.UNKNOWN_COLUMN_RUNTIME),
    ("no such table: musicians", FeedbackKind.UNKNOWN_TABLE_RUNTIME),
    # Exp 07: DuckPGQ path incoherente
    (
        "Binder Error: Label Company is not registered as a source reference "
        "for edge pattern of table LivesIn",
        FeedbackKind.PATH_INCOHERENT_RUNTIME,
    ),
    # Exp 07b: DuckPGQ vertex sin label
    (
        "Constraint Error: All patterns must bind to a label",
        FeedbackKind.DUCKPGQ_VERTEX_LABEL_REQUIRED,
    ),
    # DuckPGQ edge sin variable (originalmente observado en Fase 0)
    (
        "Constraint Error: All patterns must bind to a variable, "
        "knows is missing a variable",
        FeedbackKind.DUCKPGQ_EDGE_VARIABLE_REQUIRED,
    ),
    # Catch-all: mensaje vacío o no clasificable
    ("", FeedbackKind.RUNTIME_OTHER),
    ("some completely opaque error message", FeedbackKind.RUNTIME_OTHER),
]


@pytest.mark.parametrize("message,expected_kind", _OBSERVED)
def test_classify_observed_messages(message: str, expected_kind: FeedbackKind) -> None:
    assert classify_execution_error(message) == expected_kind


def test_descriptor_from_execution_error_includes_hint() -> None:
    msg = "Binder Error: column X must appear in the GROUP BY clause"
    descriptor = descriptor_from_execution_error(msg)
    assert descriptor.kind == FeedbackKind.GROUP_BY_VIOLATION
    assert descriptor.stage == "execution"
    assert "GROUP BY" in descriptor.hint
    # El mensaje crudo queda en raw_excerpt para registro
    assert descriptor.raw_excerpt is not None
    assert "GROUP BY" in descriptor.raw_excerpt


def test_descriptor_truncates_long_messages() -> None:
    long_msg = "Some long error " + ("X" * 500)
    descriptor = descriptor_from_execution_error(long_msg)
    assert descriptor.raw_excerpt is not None
    assert len(descriptor.raw_excerpt) <= 200


def test_descriptor_from_none_message_is_other() -> None:
    descriptor = descriptor_from_execution_error("")
    assert descriptor.kind == FeedbackKind.RUNTIME_OTHER


# ---------------------------------------------------------------------------
# Conversión de VerificationError a FeedbackDescriptor
# ---------------------------------------------------------------------------


def test_descriptors_from_verifier_errors_known_kinds() -> None:
    errors = [
        VerificationError(
            kind="unknown_column",
            message="columna 'email' no resuelve en ningún binding",
        ),
        VerificationError(
            kind="path_step_incoherent",
            message="path step (Company:Person)->lives_in",
        ),
        VerificationError(
            kind="type_mismatch_aggregate",
            message="AVG sobre TEXT",
        ),
    ]
    descriptors = descriptors_from_verifier_errors(errors)
    kinds = [d.kind for d in descriptors]
    assert kinds == [
        FeedbackKind.UNKNOWN_COLUMN,
        FeedbackKind.PATH_STEP_INCOHERENT,
        FeedbackKind.TYPE_MISMATCH_AGGREGATE,
    ]
    # Todos deben tener stage="verifier"
    assert all(d.stage == "verifier" for d in descriptors)
    # Todos deben tener hint no vacío
    assert all(d.hint for d in descriptors)


def test_descriptors_from_verifier_errors_unknown_kind_falls_back() -> None:
    """Si aparece un kind que el enum no contempla, se usa RUNTIME_OTHER y se
    preserva la información en hint y raw_excerpt para no perder rastro."""
    errors = [
        VerificationError(
            kind="some_brand_new_kind",
            message="detalle del fallo",
        )
    ]
    descriptors = descriptors_from_verifier_errors(errors)
    assert len(descriptors) == 1
    assert descriptors[0].kind == FeedbackKind.RUNTIME_OTHER
    assert "some_brand_new_kind" in descriptors[0].hint
    assert descriptors[0].raw_excerpt == "detalle del fallo"


def test_descriptor_is_hashable_and_immutable() -> None:
    """FeedbackDescriptor es frozen — debe ser hashable e inmutable."""
    d1 = FeedbackDescriptor.from_kind(FeedbackKind.UNKNOWN_COLUMN, stage="verifier")
    d2 = FeedbackDescriptor.from_kind(FeedbackKind.UNKNOWN_COLUMN, stage="verifier")
    assert d1 == d2
    assert hash(d1) == hash(d2)
    with pytest.raises(Exception):
        d1.kind = FeedbackKind.RUNTIME_OTHER  # type: ignore[misc]
