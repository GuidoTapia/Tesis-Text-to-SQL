"""
Experimento 09 — bucle de retroalimentación sobre corpus relacional.

Sanity check del bucle sobre el régimen donde el verificador estructural
ya tiene paridad con el MVP (experimento 02 mostró 99/99 IRs limpias en
el verificador). El experimento usa un subconjunto de veinte preguntas
de Spider —diez que fallaron en ejecución sobre DuckDB estricto en el
experimento 02 y diez que ejecutaron correctamente— y aplica el bucle
de retroalimentación con tres iteraciones máximas.

La pregunta operativa es complementaria a la del experimento 08: ¿el
bucle de feedback es capaz de rescatar fallos del régimen relacional,
particularmente los problemas de tipos y de calidad de datos que el
DuckDB estricto expone? ¿Y mantiene los controles intactos sin
introducir falsos rescates?

Mismo modelo (Haiku 4.5), mismo motor (DuckDB sobre Spider sqlite vía
extensión sqlite). El sistema prompt es el del experimento 05 adaptado
para incluir feedback estructurado.
"""

from __future__ import annotations

import json
import os
import random
import sys
import time
from collections import Counter
from pathlib import Path

from anthropic import Anthropic
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from core.feedback import (  # noqa: E402
    answer_with_feedback,
    render_descriptors_for_prompt,
)
from core.ir.json_schema import IR_TOOL_INPUT_SCHEMA  # noqa: E402
from core.ir.schema import ProjectedSchema, from_spider_tables  # noqa: E402
from evaluation._helpers import (  # noqa: E402
    MODEL_DEFAULT,
    execute_on_db,
    schema_as_prompt,
    write_results,
)

SPIDER = ROOT / "corpus" / "spider_bird"
TABLES = SPIDER / "tables.json"
DB_ROOT = SPIDER / "database"
RUNS_DIR = ROOT / "evaluation" / "runs"
MAX_ITERATIONS = 3
SEED = 42


SYSTEM_PROMPT = """\
Sos un compositor de IR-SQL/PGQ. Recibís un esquema relacional y una pregunta
en lenguaje natural. Tu única salida válida es invocar el tool `submit_query`
con la IR que responde la pregunta.

Reglas operativas:

1. Sólo invocás el tool. Nunca devolvés texto explicativo, código SQL, ni
   bloques markdown.
2. CRÍTICO: el campo `query` del input debe ser un OBJETO JSON anidado, NO
   un string que contenga JSON.
3. Cada nodo de la IR tiene un campo `type` con el nombre exacto de la
   dataclass. El campo `from_` lleva guion bajo final.
4. Las colecciones siempre son arrays de objetos JSON.
5. Toda referencia a tabla o columna debe usar nombres que existen en el
   esquema dado.
6. Las consultas deben ser válidas para SQLite/DuckDB.
"""


def _ir_payload_from_response(response) -> dict | None:
    for block in response.content:
        if getattr(block, "type", None) == "tool_use" and block.name == "submit_query":
            return block.input
    return None


def _make_emit_ir(client: Anthropic, model: str, db_id: str):
    """Construye el callable emit_ir para el orquestador. Recibe el db_id
    explícito para evitar ambigüedades cuando dos bases comparten nombres
    de tabla."""

    raw = json.loads(TABLES.read_text())
    entry = next(e for e in raw if e["db_id"] == db_id)
    schema_str = schema_as_prompt(entry)

    def emit_ir(question: str, schema: ProjectedSchema, descriptors: tuple):

        feedback = render_descriptors_for_prompt(descriptors)
        user_msg = f"Esquema relacional:\n{schema_str}\n\nPregunta: {question}"
        if feedback:
            user_msg += (
                f"\n\n{feedback}\n\n"
                "Reformulá la IR teniendo en cuenta estas señales. "
                "Invocá el tool `submit_query`."
            )
        else:
            user_msg += (
                "\n\nInvocá el tool `submit_query` con la IR que responde la pregunta."
            )

        response = client.messages.create(
            model=model,
            max_tokens=2048,
            temperature=0,
            system=SYSTEM_PROMPT,
            tools=[
                {
                    "name": "submit_query",
                    "description": (
                        "Submitir la IR-SQL/PGQ que responde la pregunta del usuario."
                    ),
                    "input_schema": IR_TOOL_INPUT_SCHEMA,
                }
            ],
            tool_choice={"type": "tool", "name": "submit_query"},
            messages=[{"role": "user", "content": user_msg}],
        )
        emit_ir._last_usage = response.usage  # type: ignore[attr-defined]

        payload = _ir_payload_from_response(response)
        if payload is None:
            return {"type": "Frankenstein"}
        query = payload.get("query")
        if isinstance(query, str):
            try:
                query = json.loads(query)
            except json.JSONDecodeError:
                return {"type": "Frankenstein"}
        return query

    emit_ir._last_usage = None  # type: ignore[attr-defined]
    return emit_ir


def _build_subset() -> list[dict]:
    """Selecciona 10 preguntas que fallaron en exp 02 + 10 que ejecutaron.

    Si no hay JSON de exp 02 disponible, cae a un sample aleatorio de las
    primeras 20 preguntas del corpus."""
    src_runs = sorted(RUNS_DIR.glob("experiment_02_*.json"))
    if not src_runs:
        # Fallback: tomar 20 del corpus al azar
        dev = json.loads((SPIDER / "dev.json").read_text())
        rng = random.Random(SEED)
        return [{"q": q, "exp02_executes": None} for q in rng.sample(dev, 20)]

    data = json.loads(src_runs[-1].read_text())
    failed = [r for r in data["results"] if not r["executes"]]
    succeeded = [r for r in data["results"] if r["executes"]]
    rng = random.Random(SEED)
    n_fail = min(10, len(failed))
    n_ok = min(10, len(succeeded))
    failed_sample = rng.sample(failed, n_fail) if failed else []
    succeeded_sample = rng.sample(succeeded, n_ok)
    out = []
    for r in failed_sample + succeeded_sample:
        out.append(
            {
                "q": {"db_id": r["db_id"], "question": r["question"], "query": r["gold_sql"]},
                "exp02_executes": r["executes"],
                "exp02_predicted_sql": r["predicted_sql"],
            }
        )
    return out


def main() -> int:
    load_dotenv(ROOT / ".env")
    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        print("ERROR: ANTHROPIC_API_KEY no disponible", file=sys.stderr)
        return 2
    model = os.environ.get("ANTHROPIC_MODEL", MODEL_DEFAULT)

    subset = _build_subset()
    print(f"corpus seleccionado: {len(subset)} preguntas")
    print(
        f"  fallaron en exp 02: "
        f"{sum(1 for s in subset if s['exp02_executes'] is False)}"
    )
    print(
        f"  ejecutaron en exp 02: "
        f"{sum(1 for s in subset if s['exp02_executes'] is True)}"
    )
    print()

    client = Anthropic(api_key=api_key)

    rows: list[dict] = []
    t0 = time.time()
    in_tok_total = out_tok_total = 0

    for i, item in enumerate(subset, 1):
        q = item["q"]
        db_id = q["db_id"]
        schema_rel = from_spider_tables(TABLES, db_id)
        schema = ProjectedSchema(relational=schema_rel, graphs=())
        emit_ir = _make_emit_ir(client, model, db_id)

        in_tok_q = out_tok_q = 0

        def emit_ir_with_tracking(question, schema_arg, descriptors):
            nonlocal in_tok_q, out_tok_q
            payload = emit_ir(question, schema_arg, descriptors)
            usage = emit_ir._last_usage  # type: ignore[attr-defined]
            if usage is not None:
                in_tok_q += usage.input_tokens
                out_tok_q += usage.output_tokens
            return payload

        def execute(sql: str):
            ok, err = execute_on_db(DB_ROOT, db_id, sql, engine="duckdb")
            return ok, err, None

        result = answer_with_feedback(
            question=q["question"],
            schema=schema,
            emit_ir=emit_ir_with_tracking,
            execute=execute,
            max_iterations=MAX_ITERATIONS,
        )
        in_tok_total += in_tok_q
        out_tok_total += out_tok_q

        attempts_record = []
        for a in result.attempts:
            attempts_record.append(
                {
                    "iteration": a.iteration,
                    "failed_stage": a.failed_stage,
                    "descriptor_kinds": [d.kind.value for d in a.descriptors],
                    "compiled_sql": a.compiled_sql,
                }
            )

        # Categorizamos según el outcome de exp 02 para la comparación
        prior = item["exp02_executes"]
        category = (
            "rescue_candidate" if prior is False else "should_still_pass"
        )

        rows.append(
            {
                "idx": i,
                "db_id": db_id,
                "question": q["question"],
                "exp02_executes": prior,
                "category": category,
                "success": result.success,
                "n_iterations": result.n_iterations,
                "final_sql": result.final_sql,
                "attempts": attempts_record,
                "tokens_in": in_tok_q,
                "tokens_out": out_tok_q,
            }
        )

        flag = (
            f"OK iter={result.n_iterations}"
            if result.success
            else f"FAIL after {result.n_iterations} iter"
        )
        last_kind = ""
        if result.attempts and result.attempts[-1].descriptors:
            last_kind = " " + result.attempts[-1].descriptors[-1].kind.value
        print(f"[{i:2d}/{len(subset)}] {db_id:30s} {category:18s} {flag}{last_kind}")

    elapsed = time.time() - t0

    rescue_candidates = [r for r in rows if r["category"] == "rescue_candidate"]
    should_pass = [r for r in rows if r["category"] == "should_still_pass"]
    rescued = sum(1 for r in rescue_candidates if r["success"])
    still_pass = sum(1 for r in should_pass if r["success"])

    iter_dist_rescue = Counter(
        r["n_iterations"] for r in rescue_candidates if r["success"]
    )
    iter_dist_pass = Counter(
        r["n_iterations"] for r in should_pass if r["success"]
    )

    print()
    print("=" * 72)
    print("Resumen del experimento 09 (bucle de retroalimentación sobre Spider)")
    print("=" * 72)
    print(f"total preguntas               : {len(rows)}")
    print(f"max_iterations               : {MAX_ITERATIONS}")
    print(f"tiempo total                  : {elapsed:.1f}s")
    print(f"tokens (in/out)               : {in_tok_total}/{out_tok_total}")
    print()
    print(
        f"rescue_candidates (fallaban en exp 02) éxito : "
        f"{rescued}/{len(rescue_candidates)}"
    )
    print(
        f"should_still_pass (ejecutaban en exp 02) éxito: "
        f"{still_pass}/{len(should_pass)}"
    )
    print()
    print("Distribución de iteraciones (entre los exitosos):")
    print(f"  {'iter':5s}  rescue  pass")
    for n in sorted(set(iter_dist_rescue) | set(iter_dist_pass)):
        print(
            f"  {n:5d}  {iter_dist_rescue.get(n, 0):>6d}  "
            f"{iter_dist_pass.get(n, 0):>4d}"
        )

    failures = [r for r in rows if not r["success"]]
    if failures:
        print()
        print("Fallos no rescatados:")
        for f in failures:
            last_kinds = (
                f["attempts"][-1]["descriptor_kinds"] if f["attempts"] else []
            )
            print(
                f"  {f['db_id']:30s} category={f['category']:18s} kinds={last_kinds}"
            )

    out_path = write_results(
        RUNS_DIR,
        "experiment_09",
        {
            "metadata": {
                "model": model,
                "engine": "duckdb",
                "n_total": len(rows),
                "max_iterations": MAX_ITERATIONS,
                "elapsed_s": round(elapsed, 2),
                "tokens": {"input": in_tok_total, "output": out_tok_total},
            },
            "results": rows,
        },
    )
    print()
    print(f"resultados guardados en: {out_path.relative_to(ROOT)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
