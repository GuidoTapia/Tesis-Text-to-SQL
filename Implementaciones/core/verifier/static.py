"""
Verificador estático mínimo (MVP) — Fase 4 del plan.

Dada una consulta SQL y un esquema representado como diccionario
``{tabla: [columnas]}``, detecta dos clases de inconsistencia sin ejecutar la
consulta:

1. tablas referenciadas que no existen en el esquema.
2. columnas referenciadas que no pertenecen a ninguna tabla del esquema.

No es un verificador semántico completo: no valida tipos, alcance de alias,
unicidad de funciones agregadas, ni corrección del JOIN. Su propósito es
detectar la forma más común de alucinación de esquema que cometen los LLM
antes de la ejecución.

La versión completa del verificador operará sobre IR-SQL/PGQ y cubrirá
validaciones adicionales (alcance de variables, cardinalidades, consistencia
entre PGQ y su traducción a SQL).
"""

from __future__ import annotations

from typing import Iterable

import sqlglot
from sqlglot import exp


Schema = dict[str, list[str]]


def _normalize(name: str) -> str:
    return name.lower()


def _all_columns(schema: Schema) -> set[str]:
    return {_normalize(c) for cols in schema.values() for c in cols}


def verify_sql(sql: str, schema: Schema, dialect: str = "duckdb") -> list[str]:
    """
    Devuelve la lista de errores estáticos detectados.

    Una lista vacía indica que la consulta pasa los chequeos disponibles; no
    implica que vaya a ejecutarse sin errores ni que produzca el resultado
    esperado.
    """
    try:
        parsed = sqlglot.parse_one(sql, dialect=dialect)
    except Exception as exc:
        return [f"parse_error: {exc}"]

    errors: list[str] = []
    schema_tables = {_normalize(t) for t in schema}
    schema_columns = _all_columns(schema)

    referenced_tables: Iterable[str] = (
        _normalize(t.name) for t in parsed.find_all(exp.Table) if t.name
    )
    for t in referenced_tables:
        if t not in schema_tables:
            errors.append(f"unknown_table: {t}")

    for col in parsed.find_all(exp.Column):
        name = col.name
        if not name or name == "*":
            continue
        if _normalize(name) not in schema_columns:
            errors.append(f"unknown_column: {name}")

    return errors
