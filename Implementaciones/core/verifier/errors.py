"""
Tipos de error que produce el verificador estructural sobre la IR-SQL/PGQ.

Los errores están categorizados según las tres clases del capítulo 4 §4.5.4
de la tesis (referencial, tipos, coherencia cruzada). El campo ``kind`` es la
etiqueta canónica que los reportes y los tests filtran.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional


@dataclass(frozen=True)
class VerificationError:
    """Error estructural producido por el verificador.

    ``kind`` es una etiqueta corta y estable; la lista oficial es:

    Clase referencial:
        - ``unknown_table``
        - ``unknown_column``
        - ``unknown_qualifier``
        - ``unknown_graph``
        - ``unknown_vertex_label``
        - ``unknown_edge_label``
        - ``unknown_vertex_var``

    Clase de tipos:
        - ``type_mismatch_aggregate``
        - ``type_mismatch_arithmetic``

    Clase de coherencia cruzada:
        - ``vertex_label_without_table``
        - ``path_step_incoherent``
    """

    kind: str
    message: str
    location: Optional[str] = None  # ruta opcional en el árbol; útil en logs

    def __str__(self) -> str:
        loc = f" @ {self.location}" if self.location else ""
        return f"{self.kind}: {self.message}{loc}"
