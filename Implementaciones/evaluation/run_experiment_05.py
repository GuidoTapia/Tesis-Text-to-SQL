"""
Experimento 05 — el LLM emite IR-SQL/PGQ como JSON vía tool use.

Cierre del ciclo prometido por el cap. 4: el LLM ya no puede emitir SQL
inválido por construcción. Su única forma de responder a la pregunta es
invocar el tool ``submit_query`` con un payload que valida contra el JSON
Schema de la IR. El payload se parsea a la IR tipada, se verifica
estructuralmente contra el esquema proyectado, se compila de forma
determinista a SQL y se ejecuta sobre sqlite3.

Cada paso del pipeline puede fallar y queda registrado por separado:

- ``json_schema_failure`` — la SDK rechazó el tool call por payload inválido
  contra el schema. Esperable raro si el modelo respeta el schema.
- ``parse_failure`` — el payload pasó el schema pero parse_ir lo rechaza.
  Síntoma típico: tipos correctos pero combinación inválida (e.g., un
  campo recibió ``[null]`` donde se esperaba una colección de SelectItem).
- ``verifier_errors`` — la IR es bien formada pero contiene alucinaciones
  de esquema o errores de tipo. Ese es exactamente el caso que el cap. 4
  promete que el verificador detiene antes de la ejecución.
- ``compile_failure`` — la IR usa un nodo que el compilador todavía no
  soporta (window functions, etc.).
- ``execution_failure`` — el SQL compilado falla en sqlite3. Es el último
  guardia, semánticamente equivalente a un error que el verificador no
  pudo predecir (data quality, errores semánticos genuinos del LLM en
  lógica relacional como GROUP BY).

El experimento se compara con el experimento 04 (mismo corpus, mismo
modelo, mismo motor, pero el LLM emitía SQL directo).
"""

from __future__ import annotations

import json
import os
import sys
import time
from collections import Counter
from pathlib import Path

from anthropic import Anthropic
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from core.ir import (  # noqa: E402
    IRParseError,
    compile_query,
    parse_ir,
)
from core.ir.json_schema import IR_TOOL_INPUT_SCHEMA  # noqa: E402
from core.ir.schema import ProjectedSchema, from_spider_tables  # noqa: E402
from core.verifier.structural import verify_ir  # noqa: E402
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
CORPUS_PATH = ROOT / "corpus" / "adversarial" / "spider_decoys.json"


SYSTEM_PROMPT = """\
Sos un compositor de IR-SQL/PGQ. Recibís un esquema relacional y una pregunta
en lenguaje natural. Tu única salida válida es invocar el tool
`submit_query` con la IR que responde a la pregunta.

Reglas operativas:

1. Sólo invocás el tool. Nunca devolvés texto explicativo, código SQL, ni
   bloques markdown. La respuesta es exclusivamente el tool call.
2. CRÍTICO: el campo `query` del input debe ser un OBJETO JSON anidado, NO
   un string que contenga JSON. Por ejemplo, `{"query": {"type": "RelationalQuery", ...}}`,
   nunca `{"query": "{\\"type\\": \\"RelationalQuery\\"}"}`.
3. Cada nodo de la IR tiene un campo `type` con el nombre exacto de la
   dataclass (e.g. "RelationalQuery", "ColumnExpr", "BinaryOp"). El campo
   `from_` lleva guion bajo final.
4. Las colecciones (select, group_by, order_by, etc.) siempre son arrays
   de objetos JSON, no de strings.
5. Toda referencia a tabla o columna debe usar nombres que existen en el
   esquema dado. Si la pregunta no se puede responder con el esquema, igual
   tenés que invocar el tool con la mejor IR posible — el verificador
   estructural posterior atrapará la inconsistencia y la reportará.
6. Las consultas deben ser válidas para SQLite.

Ejemplo de invocación correcta para "How many singers do we have?":

{
  "query": {
    "type": "RelationalQuery",
    "select": [
      {"type": "SelectItem",
       "expr": {"type": "Aggregate", "name": "COUNT",
                "args": [{"type": "Star"}]}}
    ],
    "from_": {"type": "FromTable",
              "table": {"type": "TableRef", "name": "singer"}}
  }
}
"""


def _ir_payload_from_response(response) -> dict | None:
    for block in response.content:
        if getattr(block, "type", None) == "tool_use" and block.name == "submit_query":
            return block.input
    return None


def _llm_emit_ir(client: Anthropic, model: str, schema_str: str, question: str):
    """Llama al modelo con tool use y devuelve el payload del tool más usage."""
    user_msg = (
        f"Esquema relacional:\n{schema_str}\n\n"
        f"Pregunta: {question}\n\n"
        f"Invocá el tool `submit_query` con la IR que responde la pregunta."
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
                    "Submitir la IR-SQL/PGQ que responde la pregunta del usuario. "
                    "El campo `query` debe ser un objeto Query (RelationalQuery o SetOperation)."
                ),
                "input_schema": IR_TOOL_INPUT_SCHEMA,
            }
        ],
        tool_choice={"type": "tool", "name": "submit_query"},
        messages=[{"role": "user", "content": user_msg}],
    )
    payload = _ir_payload_from_response(response)
    return payload, response.usage.input_tokens, response.usage.output_tokens


def _build_schema_for_question(db_id: str) -> tuple[ProjectedSchema, str]:
    """Devuelve el esquema proyectado y un texto formateado para el prompt."""
    schema_rel = from_spider_tables(TABLES, db_id)
    raw = json.loads(TABLES.read_text())
    entry = next(e for e in raw if e["db_id"] == db_id)
    return ProjectedSchema(relational=schema_rel, graphs=()), schema_as_prompt(entry)


def main() -> int:
    load_dotenv(ROOT / ".env")
    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        print("ERROR: ANTHROPIC_API_KEY no disponible", file=sys.stderr)
        return 2
    model = os.environ.get("ANTHROPIC_MODEL", MODEL_DEFAULT)

    corpus = json.loads(CORPUS_PATH.read_text())
    client = Anthropic(api_key=api_key)
    rows: list[dict] = []
    t0 = time.time()
    in_tok = out_tok = 0

    for q in corpus["questions"]:
        db_id = q["db_id"]
        schema, schema_str = _build_schema_for_question(db_id)

        outcome: dict[str, object] = {
            "id": q["id"],
            "category": q["category"],
            "db_id": db_id,
            "question": q["question"],
            "expected_hallucination": q.get("expected_hallucination"),
            "stage_failed": None,
            "tool_called": False,
            "ir_parsed": False,
            "verifier_errors": [],
            "compiled_sql": None,
            "executes_sqlite": None,
            "sqlite_error": None,
        }

        try:
            payload, i_tok, o_tok = _llm_emit_ir(client, model, schema_str, q["question"])
            in_tok += i_tok
            out_tok += o_tok
        except Exception as exc:
            outcome["stage_failed"] = "llm_call"
            outcome["error_detail"] = f"{type(exc).__name__}: {exc}"
            rows.append(outcome)
            print(f"[{q['id']:30s}] llm_call FAIL")
            continue

        if payload is None:
            outcome["stage_failed"] = "no_tool_call"
            rows.append(outcome)
            print(f"[{q['id']:30s}] no_tool_call")
            continue
        outcome["tool_called"] = True

        # Defensa: algunos modelos envuelven la IR como string JSON dentro del
        # campo `query` en vez de como objeto anidado. Lo detectamos y
        # desempaquetamos antes de parsear.
        query_payload = payload.get("query")
        if isinstance(query_payload, str):
            outcome["string_wrapping_unwrapped"] = True
            try:
                query_payload = json.loads(query_payload)
            except json.JSONDecodeError as exc:
                outcome["stage_failed"] = "parse"
                outcome["error_detail"] = f"query is a string but not valid JSON: {exc}"
                outcome["payload_excerpt"] = json.dumps(payload)[:300]
                rows.append(outcome)
                print(f"[{q['id']:30s}] parse FAIL (string-wrapped, invalid)")
                continue

        try:
            ir_node = parse_ir(query_payload)
            outcome["ir_parsed"] = True
        except (IRParseError, KeyError, TypeError) as exc:
            outcome["stage_failed"] = "parse"
            outcome["error_detail"] = str(exc)
            outcome["payload_excerpt"] = json.dumps(payload)[:300]
            rows.append(outcome)
            print(f"[{q['id']:30s}] parse FAIL")
            continue

        verifier_errors = verify_ir(ir_node, schema)
        outcome["verifier_errors"] = [
            {"kind": e.kind, "message": e.message} for e in verifier_errors
        ]
        if verifier_errors:
            outcome["stage_failed"] = "verifier"
            rows.append(outcome)
            print(
                f"[{q['id']:30s}] verifier {len(verifier_errors)} err(s)"
            )
            continue

        try:
            sql = compile_query(ir_node)
            outcome["compiled_sql"] = sql
        except NotImplementedError as exc:
            outcome["stage_failed"] = "compile"
            outcome["error_detail"] = str(exc)
            rows.append(outcome)
            print(f"[{q['id']:30s}] compile FAIL")
            continue

        ok, err = execute_on_db(DB_ROOT, db_id, sql, engine="sqlite")
        outcome["executes_sqlite"] = ok
        outcome["sqlite_error"] = err
        if not ok:
            outcome["stage_failed"] = "execution"
        rows.append(outcome)
        flag = "OK" if ok else "FAIL"
        print(f"[{q['id']:30s}] full_pipeline exec={flag}")

    elapsed = time.time() - t0
    adv = [r for r in rows if r["category"] != "control"]
    ctrl = [r for r in rows if r["category"] == "control"]

    def _stage_counts(rs: list[dict]) -> Counter:
        return Counter(r.get("stage_failed") for r in rs)

    print()
    print("=" * 70)
    print("Resumen del experimento 05 (LLM emite IR vía tool use)")
    print("=" * 70)
    print(f"total preguntas               : {len(rows)}")
    print(f"adversariales                 : {len(adv)}")
    print(f"controles                     : {len(ctrl)}")
    print()
    print(f"tiempo total                  : {elapsed:.1f}s")
    print(f"tokens (in/out)               : {in_tok}/{out_tok}")
    print()

    print("Conteo de etapas donde se detuvo el pipeline:")
    print(f"  {'etapa':24s}  adv  ctrl")
    stages_adv = _stage_counts(adv)
    stages_ctrl = _stage_counts(ctrl)
    all_stages = set(stages_adv) | set(stages_ctrl) | {None}
    for stage in sorted(all_stages, key=lambda s: (s is None, str(s))):
        a = stages_adv.get(stage, 0)
        c = stages_ctrl.get(stage, 0)
        label = "(éxito completo)" if stage is None else stage
        print(f"  {label:24s}  {a:>3d}  {c:>4d}")

    print()
    adv_executed_clean = sum(1 for r in adv if r["stage_failed"] is None)
    adv_caught_by_verifier = sum(1 for r in adv if r["stage_failed"] == "verifier")
    ctrl_executed_clean = sum(1 for r in ctrl if r["stage_failed"] is None)
    ctrl_caught_by_verifier = sum(1 for r in ctrl if r["stage_failed"] == "verifier")
    print(
        f"adversariales que llegan a ejecución limpia: "
        f"{adv_executed_clean} / {len(adv)}"
    )
    print(
        f"adversariales atrapadas por el verificador : "
        f"{adv_caught_by_verifier} / {len(adv)}"
    )
    print(
        f"controles que llegan a ejecución limpia    : "
        f"{ctrl_executed_clean} / {len(ctrl)}"
    )
    print(
        f"controles falsamente atrapadas por verifier: "
        f"{ctrl_caught_by_verifier} / {len(ctrl)}"
    )

    out_path = write_results(
        RUNS_DIR,
        "experiment_05",
        {
            "metadata": {
                "model": model,
                "corpus": str(CORPUS_PATH.relative_to(ROOT)),
                "engine": "sqlite",
                "n_total": len(rows),
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
