"""
Experimento 07 — el LLM construye consultas PGQ vía IR.

Cierra el ciclo de la tesis sobre la dimensión grafo. Mismo pipeline que el
experimento 05 (tool use con submit_query, parse, verifier estructural,
compile, ejecución) pero el esquema disponible para el LLM ahora incluye un
property graph y las preguntas requieren bloques MATCH para responderse.

El esquema de prueba es un grafo ``social_graph`` chico pero expresivo:
tres labels de vértice (Person, City, Company) y tres labels de arista
(knows, lives_in, works_at). Las preguntas adversariales del corpus
(``corpus/adversarial/pgq_decoys.json``) fuerzan al modelo a comprometerse
con un nombre concreto en alguna de tres dimensiones donde puede alucinar:
labels de vértice, labels de arista o propiedades sobre el backing table de
un vértice. Las controles son respondibles con el grafo dado y sirven para
medir falsos positivos del verificador.

El experimento 07 es la primera evaluación cuantitativa del verificador
estructural sobre la dimensión grafo y sobre composición híbrida con un LLM
real, no con IR construida a mano como en los tests unitarios.
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

from core.ir import (  # noqa: E402
    IRParseError,
    compile_query,
    parse_ir,
)
from core.ir.json_schema import IR_TOOL_INPUT_SCHEMA  # noqa: E402
from core.ir.schema import (  # noqa: E402
    ColumnSchema,
    ProjectedSchema,
    PropertyGraphEdgeTable,
    PropertyGraphSchema,
    PropertyGraphVertexTable,
    RelationalSchema,
    TableSchema,
)
from core.verifier.structural import verify_ir  # noqa: E402
from evaluation._helpers import (  # noqa: E402
    MODEL_DEFAULT,
    projected_schema_as_prompt,
    write_results,
)

RUNS_DIR = ROOT / "evaluation" / "runs"
CORPUS_PATH = ROOT / "corpus" / "adversarial" / "pgq_decoys.json"


# ---------------------------------------------------------------------------
# Esquema declarativo y setup del grafo en DuckDB
# ---------------------------------------------------------------------------


SOCIAL_SCHEMA = ProjectedSchema(
    relational=RelationalSchema(
        tables=(
            TableSchema(
                name="Person",
                columns=(
                    ColumnSchema(name="id", type="INTEGER", is_primary_key=True),
                    ColumnSchema(name="name", type="TEXT"),
                    ColumnSchema(name="age", type="INTEGER"),
                    ColumnSchema(name="country", type="TEXT"),
                ),
            ),
            TableSchema(
                name="City",
                columns=(
                    ColumnSchema(name="id", type="INTEGER", is_primary_key=True),
                    ColumnSchema(name="name", type="TEXT"),
                    ColumnSchema(name="population", type="INTEGER"),
                ),
            ),
            TableSchema(
                name="Company",
                columns=(
                    ColumnSchema(name="id", type="INTEGER", is_primary_key=True),
                    ColumnSchema(name="name", type="TEXT"),
                    ColumnSchema(name="industry", type="TEXT"),
                ),
            ),
            TableSchema(
                name="Knows",
                columns=(
                    ColumnSchema(name="p1", type="INTEGER"),
                    ColumnSchema(name="p2", type="INTEGER"),
                ),
            ),
            TableSchema(
                name="LivesIn",
                columns=(
                    ColumnSchema(name="person_id", type="INTEGER"),
                    ColumnSchema(name="city_id", type="INTEGER"),
                ),
            ),
            TableSchema(
                name="WorksAt",
                columns=(
                    ColumnSchema(name="person_id", type="INTEGER"),
                    ColumnSchema(name="company_id", type="INTEGER"),
                ),
            ),
        )
    ),
    graphs=(
        PropertyGraphSchema(
            name="social_graph",
            vertex_tables=(
                PropertyGraphVertexTable(label="Person", table="Person", key_columns=("id",)),
                PropertyGraphVertexTable(label="City", table="City", key_columns=("id",)),
                PropertyGraphVertexTable(label="Company", table="Company", key_columns=("id",)),
            ),
            edge_tables=(
                PropertyGraphEdgeTable(
                    label="knows",
                    table="Knows",
                    source_label="Person",
                    destination_label="Person",
                    source_key=("p1",),
                    destination_key=("p2",),
                ),
                PropertyGraphEdgeTable(
                    label="lives_in",
                    table="LivesIn",
                    source_label="Person",
                    destination_label="City",
                    source_key=("person_id",),
                    destination_key=("city_id",),
                ),
                PropertyGraphEdgeTable(
                    label="works_at",
                    table="WorksAt",
                    source_label="Person",
                    destination_label="Company",
                    source_key=("person_id",),
                    destination_key=("company_id",),
                ),
            ),
        ),
    ),
)


def _setup_duckdb_graph() -> duckdb.DuckDBPyConnection:
    """Crea en memoria el property graph que las preguntas del corpus
    referencian. Las preguntas se ejecutan contra esta conexión."""
    con = duckdb.connect(":memory:")
    con.execute("INSTALL duckpgq FROM community")
    con.execute("LOAD duckpgq")

    con.execute(
        "CREATE TABLE Person (id INTEGER, name VARCHAR, age INTEGER, country VARCHAR)"
    )
    con.execute(
        "INSERT INTO Person VALUES "
        "(1, 'Alice', 30, 'AR'), (2, 'Bob', 22, 'BR'), "
        "(3, 'Carol', 45, 'AR'), (4, 'Dave', 35, 'US')"
    )

    con.execute("CREATE TABLE City (id INTEGER, name VARCHAR, population INTEGER)")
    con.execute(
        "INSERT INTO City VALUES "
        "(10, 'Buenos Aires', 15000000), (11, 'Sao Paulo', 12000000), "
        "(12, 'New York', 8000000)"
    )

    con.execute("CREATE TABLE Company (id INTEGER, name VARCHAR, industry VARCHAR)")
    con.execute(
        "INSERT INTO Company VALUES "
        "(100, 'Acme', 'tech'), (101, 'GlobalCorp', 'finance')"
    )

    con.execute("CREATE TABLE Knows (p1 INTEGER, p2 INTEGER)")
    con.execute("INSERT INTO Knows VALUES (1,2), (2,3), (3,4)")

    con.execute("CREATE TABLE LivesIn (person_id INTEGER, city_id INTEGER)")
    con.execute(
        "INSERT INTO LivesIn VALUES (1,10), (2,11), (3,10), (4,12)"
    )

    con.execute("CREATE TABLE WorksAt (person_id INTEGER, company_id INTEGER)")
    con.execute(
        "INSERT INTO WorksAt VALUES (1,100), (2,100), (3,101), (4,101)"
    )

    con.execute(
        """
        CREATE PROPERTY GRAPH social_graph
          VERTEX TABLES (Person, City, Company)
          EDGE TABLES (
            Knows SOURCE KEY (p1) REFERENCES Person (id)
                  DESTINATION KEY (p2) REFERENCES Person (id)
                  LABEL knows,
            LivesIn SOURCE KEY (person_id) REFERENCES Person (id)
                    DESTINATION KEY (city_id) REFERENCES City (id)
                    LABEL lives_in,
            WorksAt SOURCE KEY (person_id) REFERENCES Person (id)
                    DESTINATION KEY (company_id) REFERENCES Company (id)
                    LABEL works_at
          )
        """
    )
    return con


# ---------------------------------------------------------------------------
# Prompt y llamada al LLM (mismo flujo que exp 05/06)
# ---------------------------------------------------------------------------


SYSTEM_PROMPT = """\
Sos un compositor de IR-SQL/PGQ. Recibís un esquema relacional con declaraciones
de property graph y una pregunta en lenguaje natural. Tu única salida válida es
invocar el tool `submit_query` con la IR que responde a la pregunta.

Reglas operativas:

1. Sólo invocás el tool. Nunca devolvés texto explicativo, código SQL, ni
   bloques markdown.
2. CRÍTICO: el campo `query` del input debe ser un OBJETO JSON anidado, NO un
   string que contenga JSON.
3. Para responder con bloques de grafo, usá un `RelationalQuery` cuyo `from_`
   sea un `FromGraphMatch` (consulta puramente de grafo) o un `Join` cuyo lado
   sea un `FromGraphMatch` (composición híbrida). Dentro del `MatchPattern` el
   campo `graph` es el nombre exacto del property graph declarado.
4. Los edges siempre tienen variable: `EdgePattern.var` no puede ser vacío
   aunque la variable no se use en `COLUMNS`. Es restricción de DuckPGQ.
5. **CRÍTICO sobre scope de bloques de grafo**: las variables de vértice y
   arista (`a`, `b`, `k`, etc.) declaradas dentro de un `MatchPattern` SÓLO
   son visibles dentro del propio MATCH (en `where` y `columns`). NO se
   propagan al `RelationalQuery` exterior. Para que el outer SELECT acceda a
   propiedades de un vértice, esas propiedades tienen que aparecer en
   `MatchPattern.columns` (con un alias), y el outer SELECT las referencia
   como `<alias_del_FromGraphMatch>.<alias_de_la_columna>`. Por ejemplo, si
   COLUMNS expone `a.name AS friend_name`, el outer SELECT usa
   `g.friend_name`, no `a.name`.
6. Toda referencia a tabla, columna, label de vértice, label de arista o
   propiedad debe usar nombres que existen en el esquema dado. Si la pregunta
   no se puede responder con el esquema, igual tenés que invocar el tool con
   la mejor IR posible — el verificador estructural posterior atrapará la
   inconsistencia.
7. Las consultas deben ser válidas para DuckPGQ.

Ejemplo correcto para "Who does Alice know directly?" (el outer SELECT usa el
alias del bloque):

{
  "query": {
    "type": "RelationalQuery",
    "select": [
      {"type": "SelectItem",
       "expr": {"type": "ColumnExpr",
                "ref": {"type": "ColumnRef", "name": "friend", "qualifier": "g"}}}
    ],
    "from_": {
      "type": "FromGraphMatch",
      "alias": "g",
      "match": {
        "type": "MatchPattern",
        "graph": "social_graph",
        "patterns": [{
          "type": "PathPattern",
          "head": {"type": "VertexPattern", "var": "a", "label": "Person"},
          "steps": [[
            {"type": "EdgePattern", "var": "k", "label": "knows", "direction": "->"},
            {"type": "VertexPattern", "var": "b", "label": "Person"}
          ]]
        }],
        "where": {
          "type": "BinaryOp", "op": "=",
          "left": {"type": "ColumnExpr",
                   "ref": {"type": "ColumnRef", "name": "name", "qualifier": "a"}},
          "right": {"type": "Literal", "value": "Alice", "raw": "'Alice'"}
        },
        "columns": [
          {"type": "SelectItem",
           "expr": {"type": "ColumnExpr",
                    "ref": {"type": "ColumnRef", "name": "name", "qualifier": "b"}},
           "alias": "friend"}
        ]
      }
    }
  }
}

Notar tres puntos del ejemplo: (i) el WHERE usa `a.name` porque está dentro
del MATCH, (ii) COLUMNS proyecta `b.name AS friend`, (iii) el outer SELECT
usa `g.friend` (alias_del_bloque.alias_de_la_columna), no `b.name`.
"""


def _ir_payload_from_response(response) -> dict | None:
    for block in response.content:
        if getattr(block, "type", None) == "tool_use" and block.name == "submit_query":
            return block.input
    return None


def _llm_emit_ir(client: Anthropic, model: str, schema_str: str, question: str):
    user_msg = (
        f"{schema_str}\n\n"
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
                "description": "Submitir la IR-SQL/PGQ que responde la pregunta del usuario.",
                "input_schema": IR_TOOL_INPUT_SCHEMA,
            }
        ],
        tool_choice={"type": "tool", "name": "submit_query"},
        messages=[{"role": "user", "content": user_msg}],
    )
    payload = _ir_payload_from_response(response)
    return payload, response.usage.input_tokens, response.usage.output_tokens


def _execute_on_graph_db(con: duckdb.DuckDBPyConnection, sql: str):
    try:
        rows = con.execute(sql).fetchall()
        return True, None, rows
    except Exception as exc:
        return False, str(exc), None


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
    in_tok = out_tok = 0

    for q in corpus["questions"]:
        outcome: dict[str, object] = {
            "id": q["id"],
            "category": q["category"],
            "question": q["question"],
            "expected_hallucination": q.get("expected_hallucination"),
            "stage_failed": None,
            "tool_called": False,
            "ir_parsed": False,
            "verifier_errors": [],
            "compiled_sql": None,
            "executes": None,
            "rows_returned": None,
            "execution_error": None,
        }

        try:
            payload, i_tok, o_tok = _llm_emit_ir(client, model, schema_str, q["question"])
            in_tok += i_tok
            out_tok += o_tok
        except Exception as exc:
            outcome["stage_failed"] = "llm_call"
            outcome["error_detail"] = f"{type(exc).__name__}: {exc}"
            rows.append(outcome)
            print(f"[{q['id']:14s}] llm_call FAIL")
            continue

        if payload is None:
            outcome["stage_failed"] = "no_tool_call"
            rows.append(outcome)
            print(f"[{q['id']:14s}] no_tool_call")
            continue
        outcome["tool_called"] = True

        query_payload = payload.get("query")
        if isinstance(query_payload, str):
            outcome["string_wrapping_unwrapped"] = True
            try:
                query_payload = json.loads(query_payload)
            except json.JSONDecodeError as exc:
                outcome["stage_failed"] = "parse"
                outcome["error_detail"] = f"query is a string but not valid JSON: {exc}"
                rows.append(outcome)
                print(f"[{q['id']:14s}] parse FAIL (string-wrapped)")
                continue

        try:
            ir_node = parse_ir(query_payload)
            outcome["ir_parsed"] = True
        except (IRParseError, KeyError, TypeError) as exc:
            outcome["stage_failed"] = "parse"
            outcome["error_detail"] = str(exc)
            rows.append(outcome)
            print(f"[{q['id']:14s}] parse FAIL")
            continue

        try:
            verifier_errors = verify_ir(ir_node, SOCIAL_SCHEMA)
        except Exception as exc:
            outcome["stage_failed"] = "verifier_crash"
            outcome["error_detail"] = f"{type(exc).__name__}: {exc}"
            rows.append(outcome)
            print(f"[{q['id']:14s}] verifier_crash {type(exc).__name__}")
            continue
        outcome["verifier_errors"] = [
            {"kind": e.kind, "message": e.message} for e in verifier_errors
        ]
        if verifier_errors:
            outcome["stage_failed"] = "verifier"
            kinds = ", ".join(sorted({e.kind for e in verifier_errors}))
            rows.append(outcome)
            print(f"[{q['id']:14s}] verifier {len(verifier_errors)} err ({kinds})")
            continue

        try:
            sql = compile_query(ir_node)
            outcome["compiled_sql"] = sql
        except NotImplementedError as exc:
            outcome["stage_failed"] = "compile"
            outcome["error_detail"] = str(exc)
            rows.append(outcome)
            print(f"[{q['id']:14s}] compile FAIL")
            continue

        ok, err, result_rows = _execute_on_graph_db(con, sql)
        outcome["executes"] = ok
        outcome["execution_error"] = err
        if ok:
            outcome["rows_returned"] = len(result_rows or [])
        if not ok:
            outcome["stage_failed"] = "execution"
        rows.append(outcome)
        flag = f"OK ({len(result_rows or [])} filas)" if ok else "FAIL"
        print(f"[{q['id']:14s}] full_pipeline exec={flag}")

    elapsed = time.time() - t0

    adv = [r for r in rows if not r["category"].startswith("control")]
    ctrl = [r for r in rows if r["category"].startswith("control")]

    def _stage_counts(rs: list[dict]) -> Counter:
        return Counter(r.get("stage_failed") for r in rs)

    print()
    print("=" * 68)
    print("Resumen del experimento 07 (LLM emite IR de grafo)")
    print("=" * 68)
    print(f"total preguntas               : {len(rows)}")
    print(f"adversariales                 : {len(adv)}")
    print(f"controles                     : {len(ctrl)}")
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
    adv_clean = sum(1 for r in adv if r["stage_failed"] is None)
    adv_caught = sum(1 for r in adv if r["stage_failed"] == "verifier")
    ctrl_clean = sum(1 for r in ctrl if r["stage_failed"] is None)
    ctrl_caught = sum(1 for r in ctrl if r["stage_failed"] == "verifier")
    print(f"adv que llegan a ejecución limpia  : {adv_clean} / {len(adv)}")
    print(f"adv atrapadas por el verificador   : {adv_caught} / {len(adv)}")
    print(f"ctrl que llegan a ejecución limpia : {ctrl_clean} / {len(ctrl)}")
    print(f"ctrl atrapadas por verifier (FP)   : {ctrl_caught} / {len(ctrl)}")

    out_path = write_results(
        RUNS_DIR,
        "experiment_07",
        {
            "metadata": {
                "model": model,
                "corpus": str(CORPUS_PATH.relative_to(ROOT)),
                "graph": "social_graph",
                "n_total": len(rows),
                "elapsed_s": round(elapsed, 2),
                "tokens": {"input": in_tok, "output": out_tok},
            },
            "results": rows,
        },
    )
    print()
    print(f"resultados guardados en: {out_path.relative_to(ROOT)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
