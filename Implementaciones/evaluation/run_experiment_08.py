"""
Experimento 08 — bucle de retroalimentación estructurada sobre IR de grafo.

Re-ejecuta el corpus adversarial PGQ del experimento 07 pero ahora habilitando
el bucle de retroalimentación de cap. 4 §4.5.4: cuando el verificador
estructural rechaza una IR o el motor falla en ejecución, el descriptor
categórico correspondiente se reinjecta al modelo como contexto adicional
para que produzca un nuevo intento. El bucle se limita a tres iteraciones.

El experimento responde a una pregunta operativa: de los fallos que el
experimento 07 dejó como "atrapados por el verificador" o "fallidos en
ejecución", ¿cuántos quedan rescatados al permitirle al modelo reformular
con la señal categórica como contexto?

Mismo modelo (Haiku 4.5), mismo grafo (social_graph), mismo motor (DuckDB
con extensión DuckPGQ). La única diferencia metodológica con el 07 es la
introducción del orquestador y de los descriptores acumulados en el prompt.
"""

from __future__ import annotations

import json
import os
import sys
import time
from collections import Counter
from pathlib import Path

import duckdb
from anthropic import Anthropic
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from core.feedback import (  # noqa: E402
    FeedbackDescriptor,
    answer_with_feedback,
    render_descriptors_for_prompt,
)
from core.ir.json_schema import IR_TOOL_INPUT_SCHEMA  # noqa: E402
from core.ir.schema import ProjectedSchema  # noqa: E402
from evaluation._helpers import (  # noqa: E402
    MODEL_DEFAULT,
    projected_schema_as_prompt,
    write_results,
)
from evaluation.run_experiment_07 import (  # noqa: E402
    SOCIAL_SCHEMA,
    SYSTEM_PROMPT as SYSTEM_PROMPT_BASE,
    _setup_duckdb_graph,
)

RUNS_DIR = ROOT / "evaluation" / "runs"
CORPUS_PATH = ROOT / "corpus" / "adversarial" / "pgq_decoys.json"
MAX_ITERATIONS = 3


def _ir_payload_from_response(response) -> dict | None:
    for block in response.content:
        if getattr(block, "type", None) == "tool_use" and block.name == "submit_query":
            return block.input
    return None


def _make_emit_ir(client: Anthropic, model: str, schema_str: str):
    """Construye el callable emit_ir que el orquestador invoca por iteración.

    Recibe la pregunta, el esquema y la tupla de descriptores acumulados.
    Devuelve el payload IR (no envuelto) listo para parse_ir."""

    def emit_ir(question: str, schema: ProjectedSchema, descriptors: tuple):
        feedback = render_descriptors_for_prompt(descriptors)
        user_msg = f"{schema_str}\n\nPregunta: {question}"
        if feedback:
            user_msg += (
                f"\n\n{feedback}\n\n"
                "Reformulá la IR teniendo en cuenta estas señales. "
                "Invocá el tool `submit_query`."
            )
        else:
            user_msg += "\n\nInvocá el tool `submit_query` con la IR que responde la pregunta."

        response = client.messages.create(
            model=model,
            max_tokens=2048,
            temperature=0,
            system=SYSTEM_PROMPT_BASE,
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
        # Tracking de tokens vía atributo en la closure
        emit_ir._last_usage = response.usage  # type: ignore[attr-defined]

        payload = _ir_payload_from_response(response)
        if payload is None:
            # Devolvemos un payload vacío para que parse_ir falle y el
            # orquestador registre el fallo
            return {"type": "Frankenstein"}

        query = payload.get("query")
        # Defensa contra wrapping como string (mismo issue que en exp 05/07)
        if isinstance(query, str):
            try:
                query = json.loads(query)
            except json.JSONDecodeError:
                return {"type": "Frankenstein"}
        return query

    emit_ir._last_usage = None  # type: ignore[attr-defined]
    return emit_ir


def _make_execute(con: duckdb.DuckDBPyConnection):
    def execute(sql: str):
        try:
            rows = con.execute(sql).fetchall()
            return True, None, rows
        except Exception as exc:
            return False, str(exc), None

    return execute


def main() -> int:
    load_dotenv(ROOT / ".env")
    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        print("ERROR: ANTHROPIC_API_KEY no disponible", file=sys.stderr)
        return 2
    model = os.environ.get("ANTHROPIC_MODEL", MODEL_DEFAULT)

    corpus = json.loads(CORPUS_PATH.read_text())
    client = Anthropic(api_key=api_key)
    con = _setup_duckdb_graph()
    schema_str = projected_schema_as_prompt(SOCIAL_SCHEMA)

    rows: list[dict] = []
    t0 = time.time()
    in_tok_total = out_tok_total = 0

    for q in corpus["questions"]:
        emit_ir = _make_emit_ir(client, model, schema_str)
        execute = _make_execute(con)

        # Wrapeamos emit_ir para acumular tokens entre llamadas
        in_tok_q = out_tok_q = 0

        def emit_ir_with_tracking(question, schema, descriptors):
            nonlocal in_tok_q, out_tok_q
            payload = emit_ir(question, schema, descriptors)
            usage = emit_ir._last_usage  # type: ignore[attr-defined]
            if usage is not None:
                in_tok_q += usage.input_tokens
                out_tok_q += usage.output_tokens
            return payload

        result = answer_with_feedback(
            question=q["question"],
            schema=SOCIAL_SCHEMA,
            emit_ir=emit_ir_with_tracking,
            execute=execute,
            max_iterations=MAX_ITERATIONS,
        )
        in_tok_total += in_tok_q
        out_tok_total += out_tok_q

        # Registro detallado por intento para diagnóstico posterior
        attempts_record = []
        for a in result.attempts:
            attempts_record.append(
                {
                    "iteration": a.iteration,
                    "failed_stage": a.failed_stage,
                    "descriptor_kinds": [d.kind.value for d in a.descriptors],
                    "compiled_sql": a.compiled_sql,
                    "rows": a.rows,
                }
            )

        rows.append(
            {
                "id": q["id"],
                "category": q["category"],
                "question": q["question"],
                "expected_hallucination": q.get("expected_hallucination"),
                "success": result.success,
                "n_iterations": result.n_iterations,
                "final_sql": result.final_sql,
                "final_rows": result.final_rows,
                "attempts": attempts_record,
                "tokens_in": in_tok_q,
                "tokens_out": out_tok_q,
            }
        )

        # Línea de progreso
        cat_short = q["category"][:18]
        flag = (
            f"OK iter={result.n_iterations}"
            if result.success
            else f"FAIL after {result.n_iterations} iter"
        )
        last_descriptor = ""
        if result.attempts and result.attempts[-1].descriptors:
            last_descriptor = " " + result.attempts[-1].descriptors[-1].kind.value
        print(f"[{q['id']:14s}] {cat_short:20s} {flag}{last_descriptor}")

    elapsed = time.time() - t0

    adv = [r for r in rows if not r["category"].startswith("control")]
    ctrl = [r for r in rows if r["category"].startswith("control")]
    success_adv = sum(1 for r in adv if r["success"])
    success_ctrl = sum(1 for r in ctrl if r["success"])

    iter_dist_adv = Counter(r["n_iterations"] for r in adv if r["success"])
    iter_dist_ctrl = Counter(r["n_iterations"] for r in ctrl if r["success"])

    print()
    print("=" * 68)
    print("Resumen del experimento 08 (bucle de retroalimentación sobre PGQ)")
    print("=" * 68)
    print(f"total preguntas               : {len(rows)}")
    print(f"adversariales                 : {len(adv)}")
    print(f"controles                     : {len(ctrl)}")
    print(f"max_iterations               : {MAX_ITERATIONS}")
    print(f"tiempo total                  : {elapsed:.1f}s")
    print(f"tokens (in/out)               : {in_tok_total}/{out_tok_total}")
    print()
    print(f"adversariales con éxito       : {success_adv}/{len(adv)}")
    print(f"controles con éxito           : {success_ctrl}/{len(ctrl)}")
    print()
    print("Distribución de iteraciones (entre las exitosas):")
    print(f"  {'iter':5s}  adv  ctrl")
    for n in sorted(set(iter_dist_adv) | set(iter_dist_ctrl)):
        print(
            f"  {n:5d}  {iter_dist_adv.get(n, 0):>3d}  {iter_dist_ctrl.get(n, 0):>4d}"
        )

    # Diagnóstico de los fallos
    failures = [r for r in rows if not r["success"]]
    if failures:
        print()
        print("Fallos (no rescatados tras max_iterations):")
        for f in failures:
            last_kinds = (
                f["attempts"][-1]["descriptor_kinds"]
                if f["attempts"]
                else []
            )
            print(f"  {f['id']:14s} kinds={last_kinds}")

    out_path = write_results(
        RUNS_DIR,
        "experiment_08",
        {
            "metadata": {
                "model": model,
                "corpus": str(CORPUS_PATH.relative_to(ROOT)),
                "graph": "social_graph",
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
