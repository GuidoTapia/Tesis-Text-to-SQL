"""
Helpers compartidos por los scripts de experimento (`run_experiment_*.py`).

No están pensados como API pública; son utilidades reutilizables internas. El
prefijo guion-bajo en el nombre del módulo lo indica. Si en algún momento se
estabiliza un subconjunto, se puede mover a `core/`.
"""

from __future__ import annotations

import json
import re
import sqlite3
from pathlib import Path
from typing import Any

import duckdb
from anthropic import Anthropic


SYSTEM_PROMPT = (
    "Sos un traductor de lenguaje natural a SQL. "
    "Respondés exclusivamente con la consulta SQL que responde la pregunta, "
    "sin explicaciones, sin bloques de código, sin prefijos. "
    "El SQL debe ser válido para SQLite."
)

MODEL_DEFAULT = "claude-haiku-4-5-20251001"

FENCE_RE = re.compile(
    r"^```(?:sql|sqlite)?\s*\n(.*?)\n?```\s*$", re.DOTALL | re.IGNORECASE
)


def extract_sql(raw: str) -> str:
    """Quita el envoltorio markdown ```sql ... ``` si está presente."""
    text = raw.strip()
    m = FENCE_RE.match(text)
    return m.group(1).strip() if m else text


def build_schema_map(tables_path: Path) -> dict[str, dict]:
    return {e["db_id"]: e for e in json.loads(tables_path.read_text())}


def schema_as_dict(entry: dict) -> dict[str, list[str]]:
    """Formato {tabla: [columnas]} requerido por core.verifier.static.verify_sql."""
    tables = entry["table_names_original"]
    cols = entry["column_names_original"]
    out: dict[str, list[str]] = {t: [] for t in tables}
    for tbl_idx, col_name in cols:
        if tbl_idx < 0:
            continue
        out[tables[tbl_idx]].append(col_name)
    return out


def schema_as_prompt(entry: dict) -> str:
    """Formato textual breve para incluir en el prompt del LLM."""
    tables = entry["table_names_original"]
    cols = entry["column_names_original"]
    col_types = entry["column_types"]
    per_table: dict[str, list[str]] = {t: [] for t in tables}
    for (tbl_idx, col_name), ct in zip(cols, col_types):
        if tbl_idx < 0:
            continue
        per_table[tables[tbl_idx]].append(f"  {col_name} {ct.upper()}")
    blocks = [f"{t} (\n" + ",\n".join(per_table[t]) + "\n)" for t in tables]
    return "\n\n".join(blocks)


def generate_sql(
    client: Anthropic, model: str, schema_str: str, question: str
) -> tuple[str, int, int]:
    msg = client.messages.create(
        model=model,
        max_tokens=512,
        temperature=0,  # reproducibilidad — el modelo sigue teniendo varianza residual de servicio
        system=SYSTEM_PROMPT,
        messages=[
            {
                "role": "user",
                "content": f"Esquema:\n{schema_str}\n\nPregunta: {question}",
            }
        ],
    )
    return extract_sql(msg.content[0].text), msg.usage.input_tokens, msg.usage.output_tokens


def execute_on_db(
    db_root: Path, db_id: str, sql: str, engine: str = "duckdb"
) -> tuple[bool, str | None]:
    """
    Ejecuta `sql` sobre la base `db_id` y devuelve (ok, error_str).

    El parámetro `engine` selecciona el motor de ejecución:
    - "duckdb" — usa DuckDB con la extensión sqlite. Tipado estricto. Es el
      motor que se usa luego para SQL/PGQ.
    - "sqlite" — usa el módulo sqlite3 de la stdlib. Tipado laxo. Es el motor
      que la literatura de Spider usa por convención.
    """
    if engine == "duckdb":
        return _execute_duckdb(db_root, db_id, sql)
    if engine == "sqlite":
        return _execute_sqlite(db_root, db_id, sql)
    raise ValueError(f"engine debe ser 'duckdb' o 'sqlite', no {engine!r}")


def _execute_duckdb(db_root: Path, db_id: str, sql: str) -> tuple[bool, str | None]:
    sqlite_path = db_root / db_id / f"{db_id}.sqlite"
    try:
        con = duckdb.connect(":memory:")
        con.execute("INSTALL sqlite; LOAD sqlite")
        con.execute(f"ATTACH '{sqlite_path}' AS spider_db (TYPE sqlite)")
        con.execute("USE spider_db")
        con.execute(sql).fetchall()
        return True, None
    except Exception as exc:
        return False, str(exc)


def _execute_sqlite(db_root: Path, db_id: str, sql: str) -> tuple[bool, str | None]:
    sqlite_path = db_root / db_id / f"{db_id}.sqlite"
    try:
        con = sqlite3.connect(str(sqlite_path))
        try:
            con.execute(sql).fetchall()
        finally:
            con.close()
        return True, None
    except Exception as exc:
        return False, str(exc)


def write_results(out_dir: Path, prefix: str, payload: dict[str, Any]) -> Path:
    """Persiste resultados en JSON con timestamp UTC y devuelve la ruta."""
    from datetime import datetime, timezone

    out_dir.mkdir(parents=True, exist_ok=True)
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    path = out_dir / f"{prefix}_{ts}.json"
    path.write_text(json.dumps(payload, indent=2))
    return path
