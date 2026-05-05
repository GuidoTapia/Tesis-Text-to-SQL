"""
Orquestador del bucle de retroalimentación estructurada.

Materializa el régimen del cap. 4 §4.5.4: dada una pregunta y un
esquema proyectado, el orquestador llama al modelo de lenguaje para
producir una IR, la valida estructuralmente, la compila, la ejecuta y,
si alguno de esos pasos falla, traduce el fallo a un descriptor
categórico y reinjecta el descriptor al modelo en el siguiente intento.

El orquestador es agnóstico al modelo concreto: recibe un callable
``emit_ir`` que encapsula la llamada al LLM y un callable ``execute``
que encapsula la ejecución. Esta abstracción permite testear el bucle
sin LLM real (con respuestas pre-canned) y reutilizarlo entre
experimentos que difieren solo en cómo se invocan los componentes
externos.

La política de retroalimentación es estructurada en sentido estricto:
el ``emit_ir`` recibe la lista acumulada de ``FeedbackDescriptor`` con
sus ``hint`` ya en lenguaje natural; nunca se le pasa el mensaje crudo
del motor. La traducción es responsabilidad del clasificador
(``core.feedback.classifier``).
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any, Optional

from core.ir import IRParseError, compile_query, parse_ir
from core.ir.schema import ProjectedSchema
from core.verifier.structural import verify_ir

from .classifier import (
    descriptor_from_execution_error,
    descriptors_from_verifier_errors,
)
from .descriptors import FeedbackDescriptor, FeedbackKind

# Tipos de los callables esperados por el orquestador.
# - ``EmitIR`` recibe la pregunta original, el esquema proyectado y la
#   tupla acumulada de descriptores; devuelve un payload JSON
#   decodificable por ``parse_ir``.
# - ``Execute`` recibe el SQL compilado; devuelve (ok, error_msg, rows).
EmitIR = Callable[
    [str, ProjectedSchema, tuple[FeedbackDescriptor, ...]],
    dict,
]
Execute = Callable[[str], tuple[bool, Optional[str], Optional[list]]]


@dataclass(frozen=True)
class AttemptRecord:
    """Un intento individual del bucle.

    Registra qué payload entregó el modelo, en qué etapa falló (si lo
    hizo) y los descriptores que la falla produjo. Cuando la ejecución
    fue exitosa, ``compiled_sql`` y ``rows`` están poblados; en caso
    contrario, los pasos posteriores al fallo quedan en ``None``.
    """

    iteration: int
    ir_payload: Optional[dict]
    descriptors: tuple[FeedbackDescriptor, ...]
    failed_stage: Optional[str] = None
    compiled_sql: Optional[str] = None
    rows: Optional[list] = None


@dataclass(frozen=True)
class FeedbackResult:
    """Resultado del bucle completo.

    ``success`` indica si la ejecución final fue exitosa; en ese caso
    ``final_sql`` y ``final_rows`` son los valores producidos. La tupla
    ``attempts`` registra todos los intentos en orden, incluyendo los
    fallidos previos al éxito (cuando lo hubo).
    """

    success: bool
    final_sql: Optional[str]
    final_rows: Optional[list]
    attempts: tuple[AttemptRecord, ...]

    @property
    def n_iterations(self) -> int:
        return len(self.attempts)


def answer_with_feedback(
    question: str,
    schema: ProjectedSchema,
    emit_ir: EmitIR,
    execute: Execute,
    max_iterations: int = 3,
) -> FeedbackResult:
    """Ejecuta el bucle de retroalimentación hasta ``max_iterations``.

    En cada iteración aplica las cinco etapas (emit, parse, verify,
    compile, execute) y termina apenas alguna fase entrega un éxito o
    se agotan las iteraciones. Los descriptores acumulados se pasan al
    siguiente ``emit_ir`` como contexto, en orden cronológico. La
    función no muta sus argumentos.
    """
    accumulated: list[FeedbackDescriptor] = []
    attempts: list[AttemptRecord] = []

    for i in range(max_iterations):
        payload = emit_ir(question, schema, tuple(accumulated))

        # Etapa parse
        try:
            ir_node = parse_ir(payload)
        except (IRParseError, KeyError, TypeError) as exc:
            descriptor = FeedbackDescriptor.from_kind(
                FeedbackKind.SYNTAX_ERROR,
                stage="parser",
                raw_excerpt=str(exc)[:200],
            )
            accumulated.append(descriptor)
            attempts.append(
                AttemptRecord(
                    iteration=i,
                    ir_payload=payload,
                    descriptors=(descriptor,),
                    failed_stage="parser",
                )
            )
            continue

        # Etapa verify
        verifier_errors = verify_ir(ir_node, schema)
        if verifier_errors:
            new_descs = descriptors_from_verifier_errors(verifier_errors)
            accumulated.extend(new_descs)
            attempts.append(
                AttemptRecord(
                    iteration=i,
                    ir_payload=payload,
                    descriptors=tuple(new_descs),
                    failed_stage="verifier",
                )
            )
            continue

        # Etapa compile
        try:
            sql = compile_query(ir_node)
        except NotImplementedError as exc:
            descriptor = FeedbackDescriptor.from_kind(
                FeedbackKind.RUNTIME_OTHER,
                stage="compiler",
                raw_excerpt=str(exc)[:200],
            )
            accumulated.append(descriptor)
            attempts.append(
                AttemptRecord(
                    iteration=i,
                    ir_payload=payload,
                    descriptors=(descriptor,),
                    failed_stage="compiler",
                )
            )
            continue

        # Etapa execute
        ok, err, rows = execute(sql)
        if ok:
            attempts.append(
                AttemptRecord(
                    iteration=i,
                    ir_payload=payload,
                    descriptors=(),
                    compiled_sql=sql,
                    rows=rows,
                )
            )
            return FeedbackResult(
                success=True,
                final_sql=sql,
                final_rows=rows,
                attempts=tuple(attempts),
            )

        descriptor = descriptor_from_execution_error(err or "")
        accumulated.append(descriptor)
        attempts.append(
            AttemptRecord(
                iteration=i,
                ir_payload=payload,
                descriptors=(descriptor,),
                compiled_sql=sql,
                failed_stage="execution",
            )
        )

    return FeedbackResult(
        success=False,
        final_sql=None,
        final_rows=None,
        attempts=tuple(attempts),
    )


# ---------------------------------------------------------------------------
# Helper para construir el contexto textual que un emit_ir puede usar
# ---------------------------------------------------------------------------


def render_descriptors_for_prompt(descriptors: tuple[FeedbackDescriptor, ...]) -> str:
    """Genera un fragmento de texto en español que un ``emit_ir`` puede
    inyectar al prompt del LLM como historial de intentos previos.

    El formato es deliberadamente conciso: una línea por descriptor con
    su categoría y su hint. No se incluye el ``raw_excerpt`` para
    respetar la política de retroalimentación estructurada del cap. 4.
    """
    if not descriptors:
        return ""
    lines = ["Intentos anteriores produjeron las siguientes señales (no repitas los mismos errores):"]
    for i, d in enumerate(descriptors, 1):
        lines.append(f"  [{i}] etapa={d.stage}, categoría={d.kind.value}: {d.hint}")
    return "\n".join(lines)
