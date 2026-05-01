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
import sys
from pathlib import Path

from anthropic import Anthropic
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from core.verifier.static import verify_sql  # noqa: E402
from evaluation._helpers import (  # noqa: E402
    MODEL_DEFAULT,
    build_schema_map,
    execute_on_db,
    generate_sql,
    schema_as_dict,
    schema_as_prompt,
    write_results,
)

SPIDER = ROOT / "corpus" / "spider_bird"
DEV = SPIDER / "dev.json"
TABLES = SPIDER / "tables.json"
DB_ROOT = SPIDER / "database"
RUNS_DIR = ROOT / "evaluation" / "runs"

DBS = ["concert_singer", "car_1", "student_transcripts_tracking"]
N_TOTAL = 20
SEED = 42


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


def main() -> int:
    load_dotenv(ROOT / ".env")
    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        print("ERROR: ANTHROPIC_API_KEY no disponible en entorno ni en .env", file=sys.stderr)
        return 2
    model = os.environ.get("ANTHROPIC_MODEL", MODEL_DEFAULT)

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
        executes_ok, exec_error = execute_on_db(DB_ROOT, db_id, pred)

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

    out_path = write_results(
        RUNS_DIR,
        "experiment_01",
        {
            "metadata": {
                "model": model,
                "seed": SEED,
                "dbs": DBS,
                "n_total": N_TOTAL,
            },
            "results": results,
        },
    )
    print()
    print(f"resultados guardados en: {out_path.relative_to(ROOT)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
