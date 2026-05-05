"""
Clasificador de mensajes de error del motor de ejecución.

Convierte un mensaje opaco emitido por DuckDB, sqlite3 o DuckPGQ en una
``FeedbackKind`` estable que el orquestador puede usar como contexto del
reintento. Es la pieza que materializa la política de retroalimentación
estructurada del cap. 4: el modelo nunca recibe el mensaje crudo, sino
la categoría.

La implementación es por matching de expresiones regulares. Los
patrones se ordenan de más específico a más general; el primer match
gana. Si ninguno matchea, se devuelve ``RUNTIME_OTHER``.

Los patrones se derivan de mensajes observados en los experimentos
documentados en ``docs/lab_notebook.md``. Cuando aparezca un nuevo
modo de fallo, se agrega un patrón nuevo y se documenta en este módulo.
"""

from __future__ import annotations

import re
from typing import Iterable

from .descriptors import FeedbackDescriptor, FeedbackKind


# Cada entrada es (regex, kind). El orden importa: van primero los
# patrones más específicos para que no los enmascare uno general.
_PATTERNS: list[tuple[re.Pattern[str], FeedbackKind]] = [
    # DuckPGQ específicos
    (
        re.compile(r"All patterns must bind to a label", re.IGNORECASE),
        FeedbackKind.DUCKPGQ_VERTEX_LABEL_REQUIRED,
    ),
    (
        re.compile(
            r"is not registered as a (?:source|destination) reference",
            re.IGNORECASE,
        ),
        FeedbackKind.PATH_INCOHERENT_RUNTIME,
    ),
    (
        re.compile(
            r"All patterns must bind to a variable",
            re.IGNORECASE,
        ),
        FeedbackKind.DUCKPGQ_EDGE_VARIABLE_REQUIRED,
    ),
    # Encoding y conversión
    (
        re.compile(r"Could not decode to UTF-8", re.IGNORECASE),
        FeedbackKind.DATA_ENCODING_FAILURE,
    ),
    (
        re.compile(r"Conversion Error", re.IGNORECASE),
        FeedbackKind.DATA_CONVERSION_FAILURE,
    ),
    (
        re.compile(r"invalid date field format", re.IGNORECASE),
        FeedbackKind.DATA_CONVERSION_FAILURE,
    ),
    (
        re.compile(
            r"Mismatch Type Error.*column was declared as",
            re.IGNORECASE,
        ),
        FeedbackKind.DATA_CONVERSION_FAILURE,
    ),
    # Errores semánticos del motor relacional
    (
        re.compile(r"must appear in (?:the )?GROUP BY", re.IGNORECASE),
        FeedbackKind.GROUP_BY_VIOLATION,
    ),
    (
        re.compile(
            r"must (?:appear in the GROUP BY clause|be (?:used in|part of) "
            r"an? aggregate function)",
            re.IGNORECASE,
        ),
        FeedbackKind.GROUP_BY_VIOLATION,
    ),
    (
        re.compile(
            r"Cannot compare values of type|cannot be cast|"
            r"Mismatch Type Error.*type",
            re.IGNORECASE,
        ),
        FeedbackKind.TYPE_INCOMPATIBLE,
    ),
    # Referencias a nombres inexistentes detectadas por el motor
    (
        re.compile(r"no such table", re.IGNORECASE),
        FeedbackKind.UNKNOWN_TABLE_RUNTIME,
    ),
    (
        re.compile(r"no such column", re.IGNORECASE),
        FeedbackKind.UNKNOWN_COLUMN_RUNTIME,
    ),
    # Errores genéricos de sintaxis (típicamente cuando el LLM emitió prosa)
    (
        re.compile(r"syntax error|near ['\"]", re.IGNORECASE),
        FeedbackKind.SYNTAX_ERROR,
    ),
    (
        re.compile(
            r"Parser Error.*(?:syntax error|Unexpected token)",
            re.IGNORECASE,
        ),
        FeedbackKind.SYNTAX_ERROR,
    ),
]


def classify_execution_error(message: str) -> FeedbackKind:
    """Clasifica un mensaje de error del motor en su categoría canónica.

    Devuelve ``FeedbackKind.RUNTIME_OTHER`` si ningún patrón coincide.
    Mensajes vacíos o ``None`` se mapean también a ``RUNTIME_OTHER``.
    """
    if not message:
        return FeedbackKind.RUNTIME_OTHER
    for pattern, kind in _PATTERNS:
        if pattern.search(message):
            return kind
    return FeedbackKind.RUNTIME_OTHER


def descriptor_from_execution_error(message: str) -> FeedbackDescriptor:
    """Construye un ``FeedbackDescriptor`` listo para inyectar al prompt.

    Acorta el mensaje original a ``raw_excerpt`` para registro pero el
    hint que el modelo recibe es la versión categórica."""
    kind = classify_execution_error(message or "")
    excerpt = (message or "").strip()
    if len(excerpt) > 200:
        excerpt = excerpt[:197] + "..."
    return FeedbackDescriptor.from_kind(kind, stage="execution", raw_excerpt=excerpt)


def descriptors_from_verifier_errors(
    verifier_errors: Iterable,
) -> list[FeedbackDescriptor]:
    """Convierte la lista de ``VerificationError`` del verificador
    estático a ``FeedbackDescriptor``s con stage ``"verifier"``.

    El parámetro está tipado como ``Iterable`` (no como
    ``list[VerificationError]``) para no introducir un import cruzado;
    cualquier objeto con atributos ``kind`` y ``message`` calza.
    """
    out: list[FeedbackDescriptor] = []
    for e in verifier_errors:
        # Reusamos la etiqueta del verificador como nombre de FeedbackKind
        # cuando coincide; si no, caemos en RUNTIME_OTHER conservando el
        # kind del verificador como excerpt para no perder información.
        try:
            kind = FeedbackKind(e.kind) if isinstance(e.kind, str) else FeedbackKind.RUNTIME_OTHER
        except ValueError:
            # El kind del verificador no está en nuestro enum; mantenemos
            # el descriptor con stage="verifier" y un hint genérico, y
            # registramos el kind original en raw_excerpt para depuración.
            out.append(
                FeedbackDescriptor(
                    kind=FeedbackKind.RUNTIME_OTHER,
                    stage="verifier",
                    hint=(
                        f"El verificador estático rechazó la IR con clase "
                        f"{e.kind!r}: {getattr(e, 'message', '')}"
                    ),
                    raw_excerpt=getattr(e, "message", None),
                )
            )
            continue
        out.append(
            FeedbackDescriptor.from_kind(
                kind,
                stage="verifier",
                raw_excerpt=getattr(e, "message", None),
            )
        )
    return out
