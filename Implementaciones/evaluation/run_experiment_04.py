"""
Fase 5 (extensión) — evaluación adversarial del verificador.

Aplica el flujo de generación, verificación estática y ejecución sobre el
conjunto adversarial de `corpus/adversarial/spider_decoys.json`. Las preguntas
están construidas para inducir al LLM a inventar tablas o columnas
inexistentes, a diferencia del corpus aleatorio de Spider donde Haiku 4.5 no
alucina nombres con esquemas pequeños.

Métricas reportadas:

- Sobre preguntas adversariales: con qué frecuencia el LLM efectivamente
  alucina (proxy = sqlite3 falla con "no such ..."), y de esas alucinaciones
  qué fracción es atrapada por el verificador estático.
- Sobre preguntas control: con qué frecuencia el verificador produce un falso
  positivo y con qué frecuencia el sqlite3 ejecuta limpiamente.

Se usa sqlite3 como motor de ejecución (engine="sqlite") porque el costo de
DuckDB estricto sobre data sucia de Spider distorsiona el conteo de errores
del LLM (ver lab notebook, entrada 2026-05-01).

Ejecución:
    uv run python evaluation/run_experiment_04.py
"""

from __future__ import annotations

import json
import os
import re
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
TABLES = SPIDER / "tables.json"
DB_ROOT = SPIDER / "database"
RUNS_DIR = ROOT / "evaluation" / "runs"
CORPUS_PATH = ROOT / "corpus" / "adversarial" / "spider_decoys.json"

NO_SUCH_RE = re.compile(r"no such (column|table)", re.IGNORECASE)


def is_hallucination_error(err: str | None) -> bool:
    """sqlite3 reporta 'no such column: X' o 'no such table: X' cuando el SQL
    referencia un nombre que no existe en la base. Es el proxy más limpio para
    detectar que el LLM efectivamente inventó un nombre."""
    return bool(err and NO_SUCH_RE.search(err))


def main() -> int:
    load_dotenv(ROOT / ".env")
    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        print("ERROR: ANTHROPIC_API_KEY no disponible en entorno ni en .env", file=sys.stderr)
        return 2
    model = os.environ.get("ANTHROPIC_MODEL", MODEL_DEFAULT)

    corpus = json.loads(CORPUS_PATH.read_text())
    schemas = build_schema_map(TABLES)
    client = Anthropic(api_key=api_key)
    t0 = time.time()
    in_tok = out_tok = 0
    rows: list[dict] = []

    for q in corpus["questions"]:
        db_id = q["db_id"]
        schema_entry = schemas[db_id]
        pred, i_tok, o_tok = generate_sql(
            client, model, schema_as_prompt(schema_entry), q["question"]
        )
        in_tok += i_tok
        out_tok += o_tok
        verifier_errors = verify_sql(pred, schema_as_dict(schema_entry))
        ok, err = execute_on_db(DB_ROOT, db_id, pred, engine="sqlite")
        hallucinated = is_hallucination_error(err)

        # ¿el verificador menciona el nombre que esperábamos?
        expected = q.get("expected_hallucination") or ""
        verifier_caught_expected = any(
            tok and tok.lower() in e.lower()
            for tok in expected.split()
            for e in verifier_errors
        )

        rows.append(
            {
                "id": q["id"],
                "category": q["category"],
                "db_id": db_id,
                "question": q["question"],
                "expected_hallucination": q.get("expected_hallucination"),
                "predicted_sql": pred,
                "static_errors": verifier_errors,
                "executes_sqlite": ok,
                "sqlite_error": err,
                "llm_hallucinated": hallucinated,
                "verifier_flagged": bool(verifier_errors),
                "verifier_caught_expected": verifier_caught_expected,
            }
        )

        flag_v = "FLG" if verifier_errors else "ok "
        flag_h = "HAL" if hallucinated else ("FAIL" if not ok else "ok  ")
        print(f"[{q['id']:30s}] {q['category']:18s} v={flag_v} sql={flag_h}")

    elapsed = time.time() - t0

    adv = [r for r in rows if r["category"] != "control"]
    ctrl = [r for r in rows if r["category"] == "control"]

    adv_n = len(adv)
    adv_hallu = sum(1 for r in adv if r["llm_hallucinated"])
    adv_caught_any = sum(1 for r in adv if r["llm_hallucinated"] and r["verifier_flagged"])
    adv_caught_expected = sum(
        1 for r in adv if r["llm_hallucinated"] and r["verifier_caught_expected"]
    )
    adv_missed = sum(1 for r in adv if r["llm_hallucinated"] and not r["verifier_flagged"])

    ctrl_n = len(ctrl)
    ctrl_fp = sum(1 for r in ctrl if r["verifier_flagged"])
    ctrl_exec_ok = sum(1 for r in ctrl if r["executes_sqlite"])

    print()
    print("=" * 70)
    print("Resumen del experimento 04 (adversarial)")
    print("=" * 70)
    print()
    print(f"preguntas adversariales       : {adv_n}")
    print(f"  el LLM alucinó              : {adv_hallu}  (tasa = {adv_hallu / adv_n:.0%})")
    if adv_hallu:
        print(f"  verificador atrapó (cualquier): {adv_caught_any}  ({adv_caught_any / adv_hallu:.0%})")
        print(f"  verificador atrapó (esperado) : {adv_caught_expected}  ({adv_caught_expected / adv_hallu:.0%})")
        print(f"  alucinaciones no detectadas   : {adv_missed}")
    else:
        print("  el LLM no alucinó en ninguna — el corpus no fue lo suficientemente adversarial")
    print()
    print(f"preguntas control             : {ctrl_n}")
    print(f"  ejecutan en sqlite          : {ctrl_exec_ok}  ({ctrl_exec_ok / ctrl_n:.0%})")
    print(f"  falsos positivos verificador: {ctrl_fp}  ({ctrl_fp / ctrl_n:.0%})")
    print()

    by_cat: dict[str, dict[str, int]] = defaultdict(lambda: {"n": 0, "hal": 0, "caught": 0})
    for r in adv:
        c = r["category"]
        by_cat[c]["n"] += 1
        by_cat[c]["hal"] += int(r["llm_hallucinated"])
        by_cat[c]["caught"] += int(r["llm_hallucinated"] and r["verifier_flagged"])
    print("Desglose por categoría adversarial:")
    print(f"  {'categoría':22s}   n   hal   det")
    for c, d in sorted(by_cat.items()):
        print(f"  {c:22s}  {d['n']:>2d}  {d['hal']:>4d}  {d['caught']:>4d}")
    print()
    print(f"tiempo total                  : {elapsed:.1f}s")
    print(f"tokens (in/out)               : {in_tok}/{out_tok}")

    out_path = write_results(
        RUNS_DIR,
        "experiment_04",
        {
            "metadata": {
                "model": model,
                "corpus": str(CORPUS_PATH.relative_to(ROOT)),
                "engine": "sqlite",
                "n_total": len(rows),
                "n_adversarial": adv_n,
                "n_control": ctrl_n,
                "elapsed_s": round(elapsed, 2),
                "total_input_tokens": in_tok,
                "total_output_tokens": out_tok,
            },
            "results": rows,
        },
    )
    print()
    print(f"resultados guardados en: {out_path.relative_to(ROOT)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
