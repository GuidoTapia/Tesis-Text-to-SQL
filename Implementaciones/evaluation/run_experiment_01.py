"""
Fase 5 del plan — primer experimento medible.

Sobre 20 preguntas de Spider (muestreadas de 3 bases de complejidad creciente)
ejecuta el flujo completo pregunta → SQL → verificador estático → ejecución en
DuckDB. Produce un resumen con la métrica central del Paso 5.3:
porcentaje de errores de ejecución detectables de forma estática, sin correr
la consulta.

Diseño mínimo: una sola invocación de LLM por pregunta, sin reintentos ni
auto-corrección. El objetivo es un número base contra el que iterar, no un
pipeline productivo.

Ejecución:
    uv run python evaluation/run_experiment_01.py
"""

from __future__ import annotations

import json
import os
import random
import re
import sys
from datetime import datetime, timezone
from pathlib import Path

import duckdb
from anthropic import Anthropic
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from core.verifier.static import verify_sql  # noqa: E402
SPIDER = ROOT / "corpus" / "spider_bird"
DEV = SPIDER / "dev.json"
TABLES = SPIDER / "tables.json"
DB_ROOT = SPIDER / "database"
RUNS_DIR = ROOT / "evaluation" / "runs"

DBS = ["concert_singer", "car_1", "student_transcripts_tracking"]
N_TOTAL = 20
SEED = 42
MODEL_DEFAULT = "claude-haiku-4-5-20251001"

SYSTEM_PROMPT = (
    "Sos un traductor de lenguaje natural a SQL. "
    "Respondés exclusivamente con la consulta SQL que responde la pregunta, "
    "sin explicaciones, sin bloques de código, sin prefijos. "
    "El SQL debe ser válido para SQLite."
)

FENCE_RE = re.compile(r"^```(?:sql|sqlite)?\s*\n(.*?)\n?```\s*$", re.DOTALL | re.IGNORECASE)


def extract_sql(raw: str) -> str:
    """
    Extrae el cuerpo SQL de una respuesta del LLM.

    Los modelos conversacionales a veces envuelven la consulta en bloques markdown
    (```sql ... ```) incluso cuando el prompt lo prohíbe. Esta función lo tolera
    para que el verificador y el ejecutor reciban SQL parseable.
    """
    text = raw.strip()
    m = FENCE_RE.match(text)
    if m:
        return m.group(1).strip()
    return text


def build_schema_map(tables_path: Path) -> dict[str, dict]:
    return {e["db_id"]: e for e in json.loads(tables_path.read_text())}


def schema_as_dict(entry: dict) -> dict[str, list[str]]:
    tables = entry["table_names_original"]
    cols = entry["column_names_original"]
    out: dict[str, list[str]] = {t: [] for t in tables}
    for tbl_idx, col_name in cols:
        if tbl_idx < 0:
            continue
        out[tables[tbl_idx]].append(col_name)
    return out


def schema_as_prompt(entry: dict) -> str:
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


def sample_questions(dev_path: Path, dbs: list[str], total: int, seed: int) -> list[dict]:
    dev = json.loads(dev_path.read_text())
    rng = random.Random(seed)
    per_db, extra = divmod(total, len(dbs))
    picks: list[dict] = []
    for i, db in enumerate(dbs):
        pool = [q for q in dev if q["db_id"] == db]
        n = per_db + (1 if i < extra else 0)
        picks.extend(rng.sample(pool, n))
    return picks


def generate_sql(
    client: Anthropic, model: str, schema_str: str, question: str
) -> tuple[str, int, int]:
    msg = client.messages.create(
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
    raw = msg.content[0].text
    return extract_sql(raw), msg.usage.input_tokens, msg.usage.output_tokens


def execute_on_db(db_id: str, sql: str) -> tuple[bool, str | None]:
    sqlite_path = DB_ROOT / db_id / f"{db_id}.sqlite"
    try:
        con = duckdb.connect(":memory:")
        con.execute("INSTALL sqlite; LOAD sqlite")
        con.execute(f"ATTACH '{sqlite_path}' AS spider_db (TYPE sqlite)")
        con.execute("USE spider_db")
        con.execute(sql).fetchall()
        return True, None
    except Exception as exc:
        return False, str(exc)


def main() -> int:
    load_dotenv(ROOT / ".env")
    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        print("ERROR: ANTHROPIC_API_KEY no disponible en entorno ni en .env", file=sys.stderr)
        return 2
    model = os.environ.get("ANTHROPIC_MODEL", MODEL_DEFAULT)

    RUNS_DIR.mkdir(parents=True, exist_ok=True)
    schemas = build_schema_map(TABLES)
    client = Anthropic(api_key=api_key)

    samples = sample_questions(DEV, DBS, N_TOTAL, SEED)
    results: list[dict] = []

    for i, q in enumerate(samples, 1):
        db_id = q["db_id"]
        schema_entry = schemas[db_id]
        pred, in_tok, out_tok = generate_sql(
            client, model, schema_as_prompt(schema_entry), q["question"]
        )
        verifier_errors = verify_sql(pred, schema_as_dict(schema_entry))
        executes_ok, exec_error = execute_on_db(db_id, pred)

        results.append(
            {
                "id": i,
                "db_id": db_id,
                "question": q["question"],
                "gold_sql": q["query"],
                "predicted_sql": pred,
                "static_errors": verifier_errors,
                "executes": executes_ok,
                "execution_error": exec_error,
                "input_tokens": in_tok,
                "output_tokens": out_tok,
            }
        )

        static_flag = "OK" if not verifier_errors else f"{len(verifier_errors)}err"
        exec_flag = "OK " if executes_ok else "FAIL"
        print(f"[{i:2d}/{N_TOTAL}] {db_id:31s} static={static_flag:6s} exec={exec_flag}")

    passes_static = sum(1 for r in results if not r["static_errors"])
    executes = sum(1 for r in results if r["executes"])
    fails_exec = N_TOTAL - executes
    caught_by_static = sum(
        1 for r in results if not r["executes"] and r["static_errors"]
    )
    missed_by_static = sum(
        1 for r in results if not r["executes"] and not r["static_errors"]
    )

    print()
    print("=" * 60)
    print("Resumen del experimento 01")
    print("=" * 60)
    print(f"total consultas              : {N_TOTAL}")
    print(f"pasan verificación estática  : {passes_static}")
    print(f"ejecutan sin error en DuckDB : {executes}")
    print(f"fallan en ejecución          : {fails_exec}")
    print(f"  - detectadas estáticamente : {caught_by_static}")
    print(f"  - escapan al verificador   : {missed_by_static}")
    if fails_exec:
        rate = caught_by_static / fails_exec
        print(f"tasa de detección estática   : {rate:.1%}")
    else:
        print("sin errores de ejecución — sin tasa de detección que calcular")

    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    out_path = RUNS_DIR / f"experiment_01_{ts}.json"
    out_path.write_text(
        json.dumps(
            {
                "metadata": {
                    "model": model,
                    "seed": SEED,
                    "dbs": DBS,
                    "n_total": N_TOTAL,
                    "generated_at": ts,
                },
                "results": results,
            },
            indent=2,
        )
    )
    print()
    print(f"resultados guardados en: {out_path.relative_to(ROOT)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
