"""
Paso 3.2 del plan — Exploración inicial de Spider desde DuckDB.

Valida que DuckDB puede leer bases de datos SQLite nativas de Spider y que las
preguntas del split de desarrollo se cargan correctamente para una base de datos
particular.

Ejecución:
    uv run python notebooks/01_explore_spider.py
"""

from __future__ import annotations

import json
from pathlib import Path

import duckdb

SPIDER_ROOT = Path(__file__).resolve().parent.parent / "corpus" / "spider_bird"
DB_NAME = "concert_singer"
SQLITE_PATH = SPIDER_ROOT / "database" / DB_NAME / f"{DB_NAME}.sqlite"
DEV_JSON_PATH = SPIDER_ROOT / "dev.json"


def attach_spider_db(sqlite_path: Path) -> duckdb.DuckDBPyConnection:
    con = duckdb.connect(":memory:")
    con.execute("INSTALL sqlite; LOAD sqlite")
    con.execute(f"ATTACH '{sqlite_path}' AS spider_db (TYPE sqlite)")
    return con


def list_tables(con: duckdb.DuckDBPyConnection) -> list[str]:
    rows = con.execute(
        "SELECT table_name FROM information_schema.tables "
        "WHERE table_catalog = 'spider_db' ORDER BY table_name"
    ).fetchall()
    return [r[0] for r in rows]


def sample_questions(dev_path: Path, db_id: str, limit: int) -> list[dict]:
    dev = json.loads(dev_path.read_text())
    return [q for q in dev if q["db_id"] == db_id][:limit]


def main() -> None:
    assert SQLITE_PATH.exists(), f"no existe {SQLITE_PATH}"
    assert DEV_JSON_PATH.exists(), f"no existe {DEV_JSON_PATH}"

    con = attach_spider_db(SQLITE_PATH)
    tables = list_tables(con)
    print(f"[{DB_NAME}] tablas descubiertas vía DuckDB+SQLite:")
    for t in tables:
        print(f"  - {t}")

    print()
    samples = sample_questions(DEV_JSON_PATH, DB_NAME, limit=3)
    print(f"primeras {len(samples)} preguntas de dev.json sobre {DB_NAME}:")
    for q in samples:
        print(f"  Q: {q['question']}")
        print(f"  SQL: {q['query']}")
        print()

    first_table = tables[0]
    count = con.execute(f"SELECT COUNT(*) FROM spider_db.{first_table}").fetchone()[0]
    print(f"sanity check: SELECT COUNT(*) FROM spider_db.{first_table} → {count}")


if __name__ == "__main__":
    main()
