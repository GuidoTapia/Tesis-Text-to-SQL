"""
Descriptores categóricos para el bucle de retroalimentación estructurada
(cap. 4 §4.5.4, política de retroalimentación).

La tesis exige que el bucle no reinyecte al modelo de lenguaje el mensaje
de error crudo del motor, sino una etiqueta categórica que abstrae la
clase del fallo. Este módulo define el catálogo de etiquetas y la
estructura del ``FeedbackDescriptor`` que el orquestador pasa al modelo
en cada reintento.

Las clases se derivan de tres fuentes:

1. Kinds del verificador estático (``core.verifier.errors``). Cuando el
   verificador rechaza una IR, sus errores son ya descriptores
   categóricos y se pueden reinyectar tal cual al modelo.
2. Errores del motor de ejecución, clasificados a partir del mensaje.
   El clasificador (``core.feedback.classifier``) hace el mapeo desde
   mensajes opacos a etiquetas estables.
3. Salidas atípicas detectadas por inspección del resultado, no por
   error: cardinalidad cero, columnas todas nulas, etc. Son señales
   débiles que el modelo puede usar para reformular sin necesariamente
   indicar un fallo.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Optional


class FeedbackKind(str, Enum):
    """Etiqueta canónica del descriptor.

    Unifica el vocabulario del verificador estático y del clasificador de
    errores de ejecución. Hereda de ``str`` para que la comparación con
    strings sea natural y para que la serialización JSON sea trivial.
    """

    # ---- Kinds del verificador estático (familia referencial) ----
    UNKNOWN_TABLE = "unknown_table"
    UNKNOWN_COLUMN = "unknown_column"
    UNKNOWN_QUALIFIER = "unknown_qualifier"
    UNKNOWN_GRAPH = "unknown_graph"
    UNKNOWN_VERTEX_LABEL = "unknown_vertex_label"
    UNKNOWN_EDGE_LABEL = "unknown_edge_label"

    # ---- Kinds del verificador estático (familia de tipos) ----
    TYPE_MISMATCH_AGGREGATE = "type_mismatch_aggregate"
    TYPE_MISMATCH_ARITHMETIC = "type_mismatch_arithmetic"

    # ---- Kinds del verificador estático (familia coherencia cruzada) ----
    VERTEX_LABEL_WITHOUT_TABLE = "vertex_label_without_table"
    PATH_STEP_INCOHERENT = "path_step_incoherent"

    # ---- Kinds del verificador estático (familia operativa) ----
    MISSING_VERTEX_LABEL = "missing_vertex_label"

    # ---- Kinds del verificador estático (estructurales) ----
    MALFORMED_SELECT_ITEM = "malformed_select_item"
    MALFORMED_ORDER_ITEM = "malformed_order_item"

    # ---- Errores del motor que correlacionan con kinds del verificador ----
    UNKNOWN_TABLE_RUNTIME = "unknown_table_runtime"
    UNKNOWN_COLUMN_RUNTIME = "unknown_column_runtime"

    # ---- Errores semánticos del motor relacional ----
    GROUP_BY_VIOLATION = "group_by_violation"
    TYPE_INCOMPATIBLE = "type_incompatible"
    AGGREGATE_MISUSE = "aggregate_misuse"
    SYNTAX_ERROR = "syntax_error"

    # ---- Errores de calidad de datos (motor estricto sobre datos sucios) ----
    DATA_CONVERSION_FAILURE = "data_conversion_failure"
    DATA_ENCODING_FAILURE = "data_encoding_failure"

    # ---- Errores específicos de DuckPGQ ----
    PATH_INCOHERENT_RUNTIME = "path_incoherent_runtime"
    DUCKPGQ_VERTEX_LABEL_REQUIRED = "duckpgq_vertex_label_required"
    DUCKPGQ_EDGE_VARIABLE_REQUIRED = "duckpgq_edge_variable_required"

    # ---- Catch-all ----
    RUNTIME_OTHER = "runtime_other"

    # ---- Salidas atípicas (no errores) ----
    OUTPUT_ZERO_ROWS = "output_zero_rows"
    OUTPUT_ALL_NULLS = "output_all_nulls"


# Mensajes en lenguaje natural pensados para el modelo. La idea es que el
# orquestador inyecte el ``hint`` correspondiente en el prompt del reintento,
# no el mensaje crudo del motor. Los hints están redactados en español
# técnico y son intencionalmente cortos para no inflar el contexto.
_HINTS: dict[FeedbackKind, str] = {
    # --- Kinds del verificador estático ---
    FeedbackKind.UNKNOWN_TABLE: (
        "La IR referencia una tabla que no existe en el esquema. Verificá "
        "que el nombre coincida exactamente con alguna tabla declarada."
    ),
    FeedbackKind.UNKNOWN_COLUMN: (
        "La IR referencia una columna que no existe en su tabla. Verificá "
        "que la columna esté declarada en el esquema y que el qualifier "
        "apunte a la tabla correcta."
    ),
    FeedbackKind.UNKNOWN_QUALIFIER: (
        "Un qualifier de columna no resuelve a tabla, vértice ni alias de "
        "GRAPH_TABLE. Asegurate que cada calificador corresponda a un "
        "binding declarado en el FROM o en un MATCH."
    ),
    FeedbackKind.UNKNOWN_GRAPH: (
        "El nombre del property graph en MatchPattern no aparece en el "
        "catálogo. Usá el nombre exacto de un grafo declarado."
    ),
    FeedbackKind.UNKNOWN_VERTEX_LABEL: (
        "Un VertexPattern usa un label que no está declarado en el grafo. "
        "Limitá los labels a los que aparecen en VERTEX TABLES."
    ),
    FeedbackKind.UNKNOWN_EDGE_LABEL: (
        "Un EdgePattern usa un label que no está declarado en el grafo. "
        "Limitá los labels a los que aparecen en EDGE TABLES."
    ),
    FeedbackKind.TYPE_MISMATCH_AGGREGATE: (
        "Una agregación AVG o SUM se aplicó sobre un operando no numérico. "
        "Aplicá la agregación sólo sobre columnas numéricas o insertá un "
        "CAST explícito."
    ),
    FeedbackKind.TYPE_MISMATCH_ARITHMETIC: (
        "Una operación aritmética recibió operandos no numéricos. Verificá "
        "los tipos de las columnas y operandos involucrados."
    ),
    FeedbackKind.VERTEX_LABEL_WITHOUT_TABLE: (
        "El grafo declara un label de vértice cuya tabla relacional asociada "
        "no existe en el esquema. Es un problema de coherencia del esquema, "
        "no de la consulta."
    ),
    FeedbackKind.PATH_STEP_INCOHERENT: (
        "Un step de un PathPattern usa una arista cuyo source o destination "
        "no coincide con los labels declarados en el grafo. Reorganizá el "
        "patrón para respetar la dirección de cada arista."
    ),
    FeedbackKind.MISSING_VERTEX_LABEL: (
        "Un VertexPattern no declara label. DuckPGQ exige label en cada "
        "patrón de vértice, incluso cuando la variable se reutiliza desde "
        "otro patrón."
    ),
    FeedbackKind.MALFORMED_SELECT_ITEM: (
        "Un item de la cláusula SELECT no es un SelectItem. Cada elemento "
        "del array select debe ser un objeto con type=SelectItem y campos "
        "expr y alias."
    ),
    FeedbackKind.MALFORMED_ORDER_ITEM: (
        "Un item de la cláusula ORDER BY no es un OrderItem. Cada elemento "
        "del array order_by debe ser un objeto con type=OrderItem."
    ),
    # --- Errores del motor ---
    FeedbackKind.UNKNOWN_TABLE_RUNTIME: (
        "El motor reportó que una tabla referenciada en la consulta no existe "
        "en la base. Verificá que el nombre y los aliases respeten el esquema."
    ),
    FeedbackKind.UNKNOWN_COLUMN_RUNTIME: (
        "El motor reportó que una columna referenciada no existe en su tabla. "
        "Verificá que cada columna calificada esté declarada en el esquema."
    ),
    FeedbackKind.GROUP_BY_VIOLATION: (
        "Una columna del SELECT no está en GROUP BY ni dentro de una "
        "agregación. Agregá la columna a GROUP BY o envolvela en una "
        "función de agregación."
    ),
    FeedbackKind.TYPE_INCOMPATIBLE: (
        "Una operación recibió operandos de tipos incompatibles. Insertá un "
        "CAST explícito o reformulá la comparación con tipos coherentes."
    ),
    FeedbackKind.AGGREGATE_MISUSE: (
        "Una función de agregación se aplicó sobre un argumento incompatible. "
        "Verificá que SUM, AVG operen sobre columnas numéricas y que MAX, "
        "MIN operen sobre tipos comparables."
    ),
    FeedbackKind.SYNTAX_ERROR: (
        "El motor no pudo parsear la consulta producida. Verificá la "
        "estructura general de la IR y que ningún campo string contenga "
        "fragmentos de SQL."
    ),
    FeedbackKind.DATA_CONVERSION_FAILURE: (
        "El motor encontró un valor que no se pudo convertir al tipo "
        "esperado (por ejemplo, una cadena vacía donde se esperaba una "
        "fecha). El problema viene de los datos de origen, no de la "
        "consulta. Considerá envolver la columna conflictiva con un "
        "manejo de nulos o evitar referenciarla directamente."
    ),
    FeedbackKind.DATA_ENCODING_FAILURE: (
        "El motor no pudo decodificar caracteres UTF-8 de una fila. El "
        "problema es de calidad de datos en la base; reformular la "
        "consulta para no proyectar la columna conflictiva puede evitar "
        "el error."
    ),
    FeedbackKind.PATH_INCOHERENT_RUNTIME: (
        "Un path multihop tiene una arista cuyo source o destination no "
        "coincide con los labels declarados en el property graph. "
        "Reorganizá el patrón para que cada arista enlace los labels que "
        "el catálogo del grafo permite."
    ),
    FeedbackKind.DUCKPGQ_VERTEX_LABEL_REQUIRED: (
        "DuckPGQ exige label en cada VertexPattern, incluso cuando la "
        "variable se reutiliza desde otro patrón. Re-declará el label en "
        "todos los vértices."
    ),
    FeedbackKind.DUCKPGQ_EDGE_VARIABLE_REQUIRED: (
        "DuckPGQ exige una variable en cada EdgePattern aunque no se use "
        "en COLUMNS. Asegurate que cada arista tenga un identificador."
    ),
    FeedbackKind.RUNTIME_OTHER: (
        "El motor reportó un error que no se pudo categorizar. Reformulá "
        "la consulta evitando construcciones inusuales."
    ),
    FeedbackKind.OUTPUT_ZERO_ROWS: (
        "La consulta ejecutó pero devolvió cero filas. Si la pregunta "
        "esperaba al menos un resultado, revisá los filtros del WHERE y "
        "los predicados del MATCH."
    ),
    FeedbackKind.OUTPUT_ALL_NULLS: (
        "La consulta ejecutó pero todas las filas tienen valores nulos en "
        "alguna columna proyectada. Esto suele indicar que un JOIN o un "
        "MATCH no encontró pares válidos."
    ),
}


def hint_for(kind: FeedbackKind) -> str:
    """Devuelve el texto en español pensado para inyectar al modelo de
    lenguaje como retroalimentación. Si la categoría no tiene hint
    específico, se usa la categoría ``RUNTIME_OTHER``."""
    return _HINTS.get(kind, _HINTS[FeedbackKind.RUNTIME_OTHER])


@dataclass(frozen=True)
class FeedbackDescriptor:
    """Descriptor categórico de un fallo o señal de salida del pipeline.

    El campo ``kind`` es la etiqueta canónica; ``hint`` es el texto que
    el orquestador inyecta al prompt del modelo en el reintento;
    ``stage`` indica dónde se originó la señal (``"verifier"`` o
    ``"execution"`` o ``"output"``). El campo ``raw_excerpt`` mantiene
    una cita corta del mensaje original solo para registro y depuración:
    no se inyecta al modelo.
    """

    kind: FeedbackKind
    stage: str
    hint: str
    raw_excerpt: Optional[str] = None

    @classmethod
    def from_kind(
        cls,
        kind: FeedbackKind,
        stage: str,
        raw_excerpt: Optional[str] = None,
    ) -> "FeedbackDescriptor":
        return cls(kind=kind, stage=stage, hint=hint_for(kind), raw_excerpt=raw_excerpt)
