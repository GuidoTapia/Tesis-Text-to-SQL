"""
Experimento 06 — mismo flujo que el experimento 05 con prompt caching del
JSON Schema de la IR.

El experimento 05 mostró que el cierre estructural funciona, pero pagó un
costo en tokens doce veces mayor que el experimento 04 porque el
``input_schema`` (~9 KB) se envía como parte del payload del tool en cada
llamada. Anthropic ofrece prompt caching: marcando el último elemento de
``tools`` con ``cache_control={"type": "ephemeral"}`` se cachea la sección
de tools y system; los hits subsiguientes pagan el diez por ciento del
costo nominal.

Este experimento es la misma corrida que el 05 (mismo corpus, mismo modelo,
mismo pipeline) con la única diferencia de incluir cache_control. Sirve
para medir el ahorro real y validar que el caching no degrada la detección.

Las métricas reportadas separan tokens regulares, tokens de creación de
cache (1.25× costo) y tokens de lectura de cache (0.10× costo) para que el
costo final sea comparable directamente con el experimento 05.
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


# Tarifas de Anthropic Haiku 4.5 al momento de la corrida (USD por MTok).
PRICE_INPUT_REGULAR = 0.80
PRICE_INPUT_CACHE_CREATE = 1.00  # 1.25× sobre regular según docs
PRICE_INPUT_CACHE_READ = 0.08  # 0.10× sobre regular
PRICE_OUTPUT = 4.00


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
    """Versión con prompt caching: marca el tool con cache_control para que la
    sección estática (system + tools) se cachee y los hits posteriores paguen
    sólo el costo del prompt incremental."""
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
                "cache_control": {"type": "ephemeral"},
            }
        ],
        tool_choice={"type": "tool", "name": "submit_query"},
        messages=[{"role": "user", "content": user_msg}],
    )
    payload = _ir_payload_from_response(response)
    usage = response.usage
    return (
        payload,
        usage.input_tokens,
        usage.output_tokens,
        getattr(usage, "cache_creation_input_tokens", 0) or 0,
        getattr(usage, "cache_read_input_tokens", 0) or 0,
    )


def _build_schema_for_question(db_id: str) -> tuple[ProjectedSchema, str]:
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
    in_tok = out_tok = cache_create = cache_read = 0

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
            payload, i_tok, o_tok, c_create, c_read = _llm_emit_ir(
                client, model, schema_str, q["question"]
            )
            in_tok += i_tok
            out_tok += o_tok
            cache_create += c_create
            cache_read += c_read
            outcome["tokens"] = {
                "input": i_tok,
                "output": o_tok,
                "cache_create": c_create,
                "cache_read": c_read,
            }
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

        # Defensa contra wrapping de IR como string (mismo issue que en exp 05)
        query_payload = payload.get("query")
        if isinstance(query_payload, str):
            outcome["string_wrapping_unwrapped"] = True
            try:
                query_payload = json.loads(query_payload)
            except json.JSONDecodeError as exc:
                outcome["stage_failed"] = "parse"
                outcome["error_detail"] = f"query is a string but not valid JSON: {exc}"
                rows.append(outcome)
                print(f"[{q['id']:30s}] parse FAIL (string-wrapped)")
                continue

        try:
            ir_node = parse_ir(query_payload)
            outcome["ir_parsed"] = True
        except (IRParseError, KeyError, TypeError) as exc:
            outcome["stage_failed"] = "parse"
            outcome["error_detail"] = str(exc)
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
                f"[{q['id']:30s}] cache(read={c_read:>5d}) verifier {len(verifier_errors)} err(s)"
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
        print(f"[{q['id']:30s}] cache(read={c_read:>5d}) full_pipeline exec={flag}")

    elapsed = time.time() - t0

    # Cost computation
    cost_input = in_tok / 1_000_000 * PRICE_INPUT_REGULAR
    cost_create = cache_create / 1_000_000 * PRICE_INPUT_CACHE_CREATE
    cost_read = cache_read / 1_000_000 * PRICE_INPUT_CACHE_READ
    cost_output = out_tok / 1_000_000 * PRICE_OUTPUT
    cost_total = cost_input + cost_create + cost_read + cost_output

    # Costo equivalente sin cache (todo regular)
    equiv_no_cache_input = in_tok + cache_create + cache_read
    cost_no_cache = (
        equiv_no_cache_input / 1_000_000 * PRICE_INPUT_REGULAR
        + cost_output
    )

    adv = [r for r in rows if r["category"] != "control"]
    ctrl = [r for r in rows if r["category"] == "control"]

    def _stage_counts(rs: list[dict]) -> Counter:
        return Counter(r.get("stage_failed") for r in rs)

    print()
    print("=" * 72)
    print("Resumen del experimento 06 (LLM emite IR vía tool use, con caching)")
    print("=" * 72)
    print(f"total preguntas               : {len(rows)}")
    print(f"tiempo total                  : {elapsed:.1f}s")
    print()
    print("Tokens:")
    print(f"  input regular               : {in_tok:>7d}")
    print(f"  cache creation (1.25×)      : {cache_create:>7d}")
    print(f"  cache read (0.10×)          : {cache_read:>7d}")
    print(f"  output                      : {out_tok:>7d}")
    print()
    print("Costo USD:")
    print(f"  input regular               : {cost_input:>7.4f}")
    print(f"  cache creation              : {cost_create:>7.4f}")
    print(f"  cache read                  : {cost_read:>7.4f}")
    print(f"  output                      : {cost_output:>7.4f}")
    print(f"  ----------------------------")
    print(f"  TOTAL                       : {cost_total:>7.4f}")
    print()
    print(f"Costo equivalente sin cache   : {cost_no_cache:>7.4f}")
    saving_pct = (1 - cost_total / cost_no_cache) * 100 if cost_no_cache else 0.0
    print(f"Ahorro relativo               : {saving_pct:>6.1f}%")
    if cache_create == 0 and cache_read == 0:
        print()
        print(
            "ATENCIÓN: cache_creation y cache_read son 0. La cuenta o el tier de "
            "API no activó el caching pese a tener cache_control declarado en el "
            "tool. La implementación está alineada con la documentación pero no "
            "se observa ahorro en esta corrida; ver lab notebook para detalle."
        )
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

    out_path = write_results(
        RUNS_DIR,
        "experiment_06",
        {
            "metadata": {
                "model": model,
                "corpus": str(CORPUS_PATH.relative_to(ROOT)),
                "engine": "sqlite",
                "n_total": len(rows),
                "elapsed_s": round(elapsed, 2),
                "tokens": {
                    "input": in_tok,
                    "output": out_tok,
                    "cache_create": cache_create,
                    "cache_read": cache_read,
                },
                "cost_usd": {
                    "actual": round(cost_total, 4),
                    "no_cache_equivalent": round(cost_no_cache, 4),
                    "saving_pct": round(saving_pct, 1),
                },
            },
            "results": rows,
        },
    )
    print()
    print(f"resultados guardados en: {out_path.relative_to(ROOT)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
