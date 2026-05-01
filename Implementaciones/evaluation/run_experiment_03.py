"""
Fase 5 (extensión) — comparación de motor de ejecución.

Reutiliza las 100 predicciones SQL del experimento 02 y las re-ejecuta sobre
las mismas bases SQLite usando el módulo `sqlite3` de la stdlib en lugar de
DuckDB. El objetivo es aislar el efecto del motor: las predicciones son
idénticas, solo cambia quién las ejecuta.

Por convención, la mayoría de la literatura sobre Spider usa `sqlite3` para
evaluar execution accuracy. Este script produce el número directamente
comparable.

Ejecución:
    uv run python evaluation/run_experiment_03.py
"""

from __future__ import annotations

import json
import sys
from collections import defaultdict
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from evaluation._helpers import execute_on_db, write_results  # noqa: E402

SPIDER = ROOT / "corpus" / "spider_bird"
DB_ROOT = SPIDER / "database"
RUNS_DIR = ROOT / "evaluation" / "runs"


def latest_experiment_02() -> Path:
    candidates = sorted(RUNS_DIR.glob("experiment_02_*.json"))
    if not candidates:
        raise FileNotFoundError(
            "no se encontraron corridas previas de experiment_02 en evaluation/runs/"
        )
    return candidates[-1]


def main() -> int:
    src_path = latest_experiment_02()
    payload = json.loads(src_path.read_text())
    src_results = payload["results"]
    src_meta = payload["metadata"]

    print(f"reutilizando predicciones de: {src_path.relative_to(ROOT)}")
    print(f"  modelo: {src_meta['model']}")
    print(f"  seed:   {src_meta['seed']}")
    print(f"  total:  {src_meta['n_total']}")
    print()

    new_results: list[dict] = []
    for r in src_results:
        sql = r["predicted_sql"]
        ok_sqlite, err_sqlite = execute_on_db(DB_ROOT, r["db_id"], sql, engine="sqlite")
        new_results.append(
            {
                "id": r["id"],
                "db_id": r["db_id"],
                "question": r["question"],
                "predicted_sql": sql,
                "duckdb_executes": r["executes"],
                "duckdb_error": r["execution_error"],
                "sqlite_executes": ok_sqlite,
                "sqlite_error": err_sqlite,
            }
        )

        flag = (
            "agree-OK"
            if r["executes"] and ok_sqlite
            else "agree-FAIL"
            if not r["executes"] and not ok_sqlite
            else "DUCKDB-only-FAIL"
            if not r["executes"] and ok_sqlite
            else "SQLITE-only-FAIL"
        )
        print(f"[{r['id']:3d}] {r['db_id']:31s} {flag}")

    n = len(new_results)
    duckdb_ok = sum(1 for r in new_results if r["duckdb_executes"])
    sqlite_ok = sum(1 for r in new_results if r["sqlite_executes"])
    both_ok = sum(1 for r in new_results if r["duckdb_executes"] and r["sqlite_executes"])
    only_duckdb = sum(
        1 for r in new_results if r["duckdb_executes"] and not r["sqlite_executes"]
    )
    only_sqlite = sum(
        1 for r in new_results if not r["duckdb_executes"] and r["sqlite_executes"]
    )
    both_fail = sum(
        1 for r in new_results if not r["duckdb_executes"] and not r["sqlite_executes"]
    )

    print()
    print("=" * 64)
    print("Resumen del experimento 03 (comparación de motores)")
    print("=" * 64)
    print(f"total consultas              : {n}")
    print(f"ejecutan en DuckDB           : {duckdb_ok}  ({duckdb_ok / n:.1%})")
    print(f"ejecutan en sqlite3          : {sqlite_ok}  ({sqlite_ok / n:.1%})")
    print()
    print(f"ambos motores OK             : {both_ok}")
    print(f"solo DuckDB falla            : {only_sqlite}")
    print(f"solo sqlite3 falla           : {only_duckdb}")
    print(f"ambos motores fallan         : {both_fail}")
    print()

    by_db: dict[str, dict[str, int]] = defaultdict(
        lambda: {"n": 0, "duckdb": 0, "sqlite": 0}
    )
    for r in new_results:
        d = by_db[r["db_id"]]
        d["n"] += 1
        d["duckdb"] += int(r["duckdb_executes"])
        d["sqlite"] += int(r["sqlite_executes"])
    print(f"  {'db_id':32s}  n  duckdb  sqlite  Δ")
    for db in src_meta["dbs"]:
        d = by_db[db]
        delta = d["sqlite"] - d["duckdb"]
        print(f"  {db:32s} {d['n']:>2d}  {d['duckdb']:>6d}  {d['sqlite']:>6d}  {delta:+d}")

    out_path = write_results(
        RUNS_DIR,
        "experiment_03",
        {
            "metadata": {
                "source": str(src_path.relative_to(ROOT)),
                "model": src_meta["model"],
                "seed": src_meta["seed"],
                "n_total": n,
            },
            "results": new_results,
        },
    )
    print()
    print(f"resultados guardados en: {out_path.relative_to(ROOT)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
