"""Bucle de retroalimentación estructurada (cap. 4 §4.5.4).

Re-exporta la API pública del paquete para que los consumidores importen
desde ``core.feedback`` sin conocer la estructura interna.
"""

from .classifier import (
    classify_execution_error,
    descriptor_from_execution_error,
    descriptors_from_verifier_errors,
)
from .descriptors import FeedbackDescriptor, FeedbackKind, hint_for
from .orchestrator import (
    AttemptRecord,
    EmitIR,
    Execute,
    FeedbackResult,
    answer_with_feedback,
    render_descriptors_for_prompt,
)

__all__ = [
    "AttemptRecord",
    "EmitIR",
    "Execute",
    "FeedbackDescriptor",
    "FeedbackKind",
    "FeedbackResult",
    "answer_with_feedback",
    "classify_execution_error",
    "descriptor_from_execution_error",
    "descriptors_from_verifier_errors",
    "hint_for",
    "render_descriptors_for_prompt",
]
