"""
Paso 3.3 del plan (adaptado) — Primera inferencia end-to-end.

Genera SQL para una pregunta de Spider usando Claude Haiku 4.5 como LLM inicial
(conforme a DECISIONS.md). Ejecuta el SQL resultante contra DuckDB+SQLite para
validar el ciclo completo NL → SQL → ejecución.

El script no guarda métricas ni iteraciones; su objetivo es únicamente confirmar
que el flujo corre para una sola pregunta. La iteración sobre más preguntas y la
medición estructurada corresponden a la Fase 5 del plan.

Prerrequisitos:
- `ANTHROPIC_API_KEY` exportada en el entorno o presente en `Implementaciones/.env`.
- Corpus Spider ya descargado en `corpus/spider_bird/` (Paso 3.1).

Ejecución:
    uv run python notebooks/02_first_inference.py
"""

from __future__ import annotations

import json
import os
import sys
from pathlib import Path

import duckdb
from anthropic import Anthropic
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parent.parent
SPIDER_ROOT = ROOT / "corpus" / "spider_bird"
DB_NAME = "concert_singer"
SQLITE_PATH = SPIDER_ROOT / "database" / DB_NAME / f"{DB_NAME}.sqlite"
DEV_JSON_PATH = SPIDER_ROOT / "dev.json"
TABLES_JSON_PATH = SPIDER_ROOT / "tables.json"

SYSTEM_PROMPT = (
    "Sos un traductor de lenguaje natural a SQL. "
    "Recibís un esquema de base de datos y una pregunta en lenguaje natural. "
    "Respondés exclusivamente con la consulta SQL que responde la pregunta, "
    "sin explicaciones, sin bloques de código, sin prefijos. "
    "El SQL debe ser válido para SQLite."
)


def load_schema(tables_path: Path, db_id: str) -> dict:
    tables = json.loads(tables_path.read_text())
    for entry in tables:
        if entry["db_id"] == db_id:
            return entry
    raise KeyError(f"db_id {db_id!r} no presente en tables.json")


def format_schema(schema_entry: dict) -> str:
    table_names = schema_entry["table_names_original"]
    column_names = schema_entry["column_names_original"]
    column_types = schema_entry["column_types"]

    per_table: dict[str, list[str]] = {t: [] for t in table_names}
    for (tbl_idx, col_name), col_type in zip(column_names, column_types):
        if tbl_idx < 0:
            continue
        per_table[table_names[tbl_idx]].append(f"  {col_name} {col_type.upper()}")

    blocks = []
    for tbl in table_names:
        blocks.append(f"{tbl} (\n" + ",\n".join(per_table[tbl]) + "\n)")
    return "\n\n".join(blocks)


def generate_sql(client: Anthropic, model: str, schema_str: str, question: str) -> str:
    message = client.messages.create(
        model=model,
        max_tokens=512,
        system=SYSTEM_PROMPT,
        messages=[
            {
                "role": "user",
                "content": f"Esquema:\n{schema_str}\n\nPregunta: {question}",
            }
        ],
    )
    return message.content[0].text.strip()


def execute_on_duckdb(sqlite_path: Path, sql: str):
    con = duckdb.connect(":memory:")
    con.execute("INSTALL sqlite; LOAD sqlite")
    con.execute(f"ATTACH '{sqlite_path}' AS spider_db (TYPE sqlite)")
    con.execute("USE spider_db")
    return con.execute(sql).fetchall()


def main() -> int:
    load_dotenv(ROOT / ".env")
    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        print("ERROR: ANTHROPIC_API_KEY no está en el entorno.", file=sys.stderr)
        print("       Exportala o crealá en Implementaciones/.env", file=sys.stderr)
        return 2

    model = os.environ.get("ANTHROPIC_MODEL", "claude-haiku-4-5-20251001")

    for path in (SQLITE_PATH, DEV_JSON_PATH, TABLES_JSON_PATH):
        if not path.exists():
            print(f"ERROR: no existe {path}", file=sys.stderr)
            return 2

    schema_entry = load_schema(TABLES_JSON_PATH, DB_NAME)
    schema_str = format_schema(schema_entry)

    dev = json.loads(DEV_JSON_PATH.read_text())
    sample = next(q for q in dev if q["db_id"] == DB_NAME)
    question = sample["question"]
    gold_sql = sample["query"]

    print(f"db_id:      {DB_NAME}")
    print(f"modelo:     {model}")
    print(f"pregunta:   {question}")
    print(f"sql (gold): {gold_sql}")
    print()

    client = Anthropic(api_key=api_key)
    predicted_sql = generate_sql(client, model, schema_str, question)
    print(f"sql (gen):  {predicted_sql}")
    print()

    try:
        rows = execute_on_duckdb(SQLITE_PATH, predicted_sql)
        print(f"ejecución: OK ({len(rows)} fila(s))")
        for row in rows[:5]:
            print(f"  {row}")
    except Exception as exc:
        print(f"ejecución: FALLÓ — {exc}")
        return 1

    return 0


if __name__ == "__main__":
    sys.exit(main())
