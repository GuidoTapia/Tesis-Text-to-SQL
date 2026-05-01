"""
Fase 5 (extensión) — segundo experimento medible.

Escala el experimento 01 a 100 preguntas distribuidas entre 6 bases de datos de
Spider de complejidad variada. Mantiene el mismo modelo (Claude Haiku 4.5),
verificador estático y motor de ejecución, pero amplía el sample para que la
métrica de detección estática tenga más peso estadístico y para exponer al
verificador a una mayor variedad de esquemas.

Diseño deliberadamente minimalista (sin reintentos, sin paralelismo, sin
caching). El énfasis está en obtener números honestos y reproducibles, no en
optimizar el costo.

Ejecución:
    uv run python evaluation/run_experiment_02.py
"""

from __future__ import annotations

import json
import os
import random
import sys
import time
from collections import defaultdict
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

DBS = [
    "concert_singer",                # 4 tablas — referencia chica
    "world_1",                       # 4 tablas — chica pero más cobertura
    "car_1",                         # 6 tablas — media
    "wta_1",                         # 3 tablas pero 44 columnas — schema "ancho"
    "dog_kennels",                   # 8 tablas — media-grande
    "student_transcripts_tracking",  # 11 tablas — grande
]
N_TOTAL = 100
SEED = 42


def sample_questions(dev_path: Path, dbs: list[str], total: int, seed: int) -> list[dict]:
    dev = json.loads(dev_path.read_text())
    rng = random.Random(seed)
    per_db, extra = divmod(total, len(dbs))
    picks: list[dict] = []
    for i, db in enumerate(dbs):
        pool = [q for q in dev if q["db_id"] == db]
        n = per_db + (1 if i < extra else 0)
        if n > len(pool):
            raise ValueError(f"se piden {n} preguntas de {db} pero solo hay {len(pool)}")
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
    t0 = time.time()
    total_in_tokens = 0
    total_out_tokens = 0

    for i, q in enumerate(samples, 1):
        db_id = q["db_id"]
        schema_entry = schemas[db_id]
        pred, in_tok, out_tok = generate_sql(
            client, model, schema_as_prompt(schema_entry), q["question"]
        )
        total_in_tokens += in_tok
        total_out_tokens += out_tok
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
        print(f"[{i:3d}/{N_TOTAL}] {db_id:31s} static={static_flag:6s} exec={exec_flag}")

    elapsed = time.time() - t0

    passes_static = sum(1 for r in results if not r["static_errors"])
    executes = sum(1 for r in results if r["executes"])
    fails_exec = N_TOTAL - executes
    caught_by_static = sum(1 for r in results if not r["executes"] and r["static_errors"])
    missed_by_static = sum(1 for r in results if not r["executes"] and not r["static_errors"])
    false_positives = sum(1 for r in results if r["executes"] and r["static_errors"])

    by_db: dict[str, dict[str, int]] = defaultdict(lambda: {"n": 0, "exec_ok": 0, "static_ok": 0, "fails": 0})
    for r in results:
        d = by_db[r["db_id"]]
        d["n"] += 1
        d["exec_ok"] += int(r["executes"])
        d["static_ok"] += int(not r["static_errors"])
        d["fails"] += int(not r["executes"])

    print()
    print("=" * 64)
    print("Resumen del experimento 02")
    print("=" * 64)
    print(f"total consultas                    : {N_TOTAL}")
    print(f"pasan verificación estática        : {passes_static}")
    print(f"ejecutan sin error en DuckDB       : {executes}")
    print(f"fallan en ejecución                : {fails_exec}")
    print(f"  - detectadas estáticamente       : {caught_by_static}")
    print(f"  - escapan al verificador         : {missed_by_static}")
    print(f"falsos positivos del verificador   : {false_positives}")
    if fails_exec:
        rate = caught_by_static / fails_exec
        print(f"tasa de detección estática         : {rate:.1%}")
    else:
        print("sin errores de ejecución — sin tasa de detección")
    print()
    print(f"tiempo total                       : {elapsed:.1f}s")
    print(f"tokens (in/out)                    : {total_in_tokens}/{total_out_tokens}")
    print()

    print("Desglose por base de datos:")
    print(f"  {'db_id':32s}  n  exec  static  fail")
    for db in DBS:
        d = by_db[db]
        print(f"  {db:32s} {d['n']:>2d}  {d['exec_ok']:>4d}  {d['static_ok']:>6d}  {d['fails']:>4d}")

    out_path = write_results(
        RUNS_DIR,
        "experiment_02",
        {
            "metadata": {
                "model": model,
                "seed": SEED,
                "dbs": DBS,
                "n_total": N_TOTAL,
                "elapsed_s": round(elapsed, 2),
                "total_input_tokens": total_in_tokens,
                "total_output_tokens": total_out_tokens,
            },
            "results": results,
        },
    )
    print()
    print(f"resultados guardados en: {out_path.relative_to(ROOT)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
