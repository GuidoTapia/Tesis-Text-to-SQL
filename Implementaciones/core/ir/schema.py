"""
Modelo de esquema relacional tipado para uso por la IR y el verificador.

Extiende el ``dict[str, list[str]]`` que usa el verificador MVP en
``core.verifier.static`` con tipos de columna y constraints básicos. El paso
de un formato al otro se hace explícito con ``to_simple_dict`` y
``from_spider_tables`` (que consume el ``tables.json`` de Spider).
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Optional
import json


@dataclass(frozen=True)
class ColumnSchema:
    name: str
    type: str  # tipo declarado (TEXT, INTEGER, REAL, NULL, BLOB, ...)
    nullable: bool = True
    is_primary_key: bool = False


@dataclass(frozen=True)
class TableSchema:
    name: str
    columns: tuple[ColumnSchema, ...]

    def find_column(self, name: str) -> Optional[ColumnSchema]:
        target = name.lower()
        for c in self.columns:
            if c.name.lower() == target:
                return c
        return None


@dataclass(frozen=True)
class RelationalSchema:
    """Conjunto de tablas con tipos y claves primarias.

    Las búsquedas por nombre son case-insensitive porque SQLite y DuckDB
    difieren en sus reglas y queremos un comportamiento consistente desde la
    IR.
    """

    tables: tuple[TableSchema, ...]

    def find_table(self, name: str) -> Optional[TableSchema]:
        target = name.lower()
        for t in self.tables:
            if t.name.lower() == target:
                return t
        return None

    def to_simple_dict(self) -> dict[str, list[str]]:
        """Compatibilidad con el formato del verificador MVP."""
        return {t.name: [c.name for c in t.columns] for t in self.tables}


def from_spider_tables(tables_json_path: Path, db_id: str) -> RelationalSchema:
    """Construye un ``RelationalSchema`` desde el ``tables.json`` de Spider."""
    raw = json.loads(tables_json_path.read_text())
    entry = next((e for e in raw if e["db_id"] == db_id), None)
    if entry is None:
        raise KeyError(f"db_id {db_id!r} no presente en {tables_json_path}")

    table_names = entry["table_names_original"]
    pk_idx = set(entry.get("primary_keys", []))
    cols_per_table: dict[int, list[ColumnSchema]] = {i: [] for i in range(len(table_names))}
    for col_idx, ((tbl_idx, col_name), col_type) in enumerate(
        zip(entry["column_names_original"], entry["column_types"])
    ):
        if tbl_idx < 0:
            continue
        cols_per_table[tbl_idx].append(
            ColumnSchema(
                name=col_name,
                type=col_type.upper(),
                nullable=True,
                is_primary_key=col_idx in pk_idx,
            )
        )
    tables = tuple(
        TableSchema(name=name, columns=tuple(cols_per_table[i]))
        for i, name in enumerate(table_names)
    )
    return RelationalSchema(tables=tables)


# ---------------------------------------------------------------------------
# Esquema de grafo de propiedad y esquema proyectado
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class PropertyGraphVertexTable:
    """Una declaración ``VERTEX TABLES (Person)`` enlaza un label de vértice
    con una tabla relacional. Es el puente que la coherencia cruzada chequea."""

    label: str
    table: str
    key_columns: tuple[str, ...] = ()


@dataclass(frozen=True)
class PropertyGraphEdgeTable:
    label: str
    table: str
    source_label: str
    destination_label: str
    source_key: tuple[str, ...] = ()
    destination_key: tuple[str, ...] = ()


@dataclass(frozen=True)
class PropertyGraphSchema:
    """Declaración de un property graph como la registra DuckPGQ en su catálogo."""

    name: str
    vertex_tables: tuple[PropertyGraphVertexTable, ...]
    edge_tables: tuple[PropertyGraphEdgeTable, ...]

    def find_vertex_label(self, label: str) -> Optional[PropertyGraphVertexTable]:
        target = label.lower()
        for v in self.vertex_tables:
            if v.label.lower() == target:
                return v
        return None

    def find_edge_label(self, label: str) -> Optional[PropertyGraphEdgeTable]:
        target = label.lower()
        for e in self.edge_tables:
            if e.label.lower() == target:
                return e
        return None


@dataclass(frozen=True)
class ProjectedSchema:
    """Σ_proj de la tesis (cap. 4 §4.5.1).

    Combina el esquema relacional y los grafos de propiedad declarados. Es el
    único contexto estructural que el verificador consume para resolver
    referencias.
    """

    relational: RelationalSchema
    graphs: tuple[PropertyGraphSchema, ...] = ()

    def find_graph(self, name: str) -> Optional[PropertyGraphSchema]:
        target = name.lower()
        for g in self.graphs:
            if g.name.lower() == target:
                return g
        return None
