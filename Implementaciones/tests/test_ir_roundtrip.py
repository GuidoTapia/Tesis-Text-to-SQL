"""
Test de cobertura — round-trip de la IR contra las predicciones reales del
experimento 02.

Criterio de cierre de la rebanada 1: la IR es expresivamente suficiente para
representar el SQL que produce un LLM frontier sobre Spider. Concretamente:

1. ``lift_sql`` debe procesar exitosamente cada predicción.
2. ``compile_query(lift_sql(sql))`` debe ser sqlglot-parseable.
3. La IR liftada del SQL recompilado debe ser estructuralmente idéntica a la
   IR original (módulo ParenExpr cosméticos).

Si alguna predicción falla, el test imprime detalles para diagnóstico y, según
el modo, falla o reporta cobertura. Por ahora corre en modo "informe": colecta
fallos y los reporta en el final.
"""

from __future__ import annotations

import json
from dataclasses import fields, is_dataclass, replace
from pathlib import Path

import pytest

from core.ir import compile_query, lift_sql
from core.ir import nodes as ir
from core.ir.lift import UnsupportedSQLError

ROOT = Path(__file__).resolve().parent.parent
RUNS_DIR = ROOT / "evaluation" / "runs"


def _strip_parens(node):
    if isinstance(node, ir.ParenExpr):
        return _strip_parens(node.inner)
    if isinstance(node, tuple):
        return tuple(_strip_parens(x) for x in node)
    if is_dataclass(node):
        kwargs = {f.name: _strip_parens(getattr(node, f.name)) for f in fields(node)}
        return replace(node, **kwargs)
    return node


def _latest_run(prefix: str) -> Path | None:
    files = sorted(RUNS_DIR.glob(f"{prefix}_*.json"))
    return files[-1] if files else None


@pytest.fixture(scope="module")
def experiment_02_predictions() -> list[dict]:
    path = _latest_run("experiment_02")
    if path is None:
        pytest.skip("no se encontró experiment_02_*.json para evaluar cobertura")
    data = json.loads(path.read_text())
    return data["results"]


def test_ir_lifts_experiment_02_predictions(experiment_02_predictions) -> None:
    """Reporta cobertura del lifter sobre las 100 predicciones de experiment_02."""

    total = len(experiment_02_predictions)
    lifted = 0
    recompiled = 0
    roundtrip_match = 0
    failures: list[tuple[int, str, str]] = []

    for r in experiment_02_predictions:
        sql = r["predicted_sql"]
        try:
            ir1 = _strip_parens(lift_sql(sql))
        except UnsupportedSQLError as e:
            failures.append((r["id"], "lift", str(e)))
            continue
        except Exception as e:
            failures.append((r["id"], f"lift-{type(e).__name__}", str(e)))
            continue
        lifted += 1

        try:
            sql2 = compile_query(ir1)
        except Exception as e:
            failures.append((r["id"], f"compile-{type(e).__name__}", str(e)))
            continue
        recompiled += 1

        try:
            ir2 = _strip_parens(lift_sql(sql2))
        except Exception as e:
            failures.append((r["id"], f"relift-{type(e).__name__}", str(e)[:120]))
            continue

        if ir1 == ir2:
            roundtrip_match += 1
        else:
            failures.append((r["id"], "ir-mismatch", sql[:80]))

    print()
    print("=" * 60)
    print("Cobertura IR sobre experiment_02 predictions")
    print("=" * 60)
    print(f"total predicciones                : {total}")
    print(f"lift exitoso                      : {lifted}  ({lifted/total:.0%})")
    print(f"compile exitoso                   : {recompiled}")
    print(f"round-trip ir1 == ir2             : {roundtrip_match}  ({roundtrip_match/total:.0%})")
    print()

    if failures:
        # Agrupar fallos por motivo
        from collections import Counter

        kinds = Counter(f[1] for f in failures)
        print("desglose de fallos por motivo:")
        for k, c in kinds.most_common():
            print(f"  {k:30s}  {c}")
        print()
        print("primeros 10 fallos:")
        for fid, kind, msg in failures[:10]:
            print(f"  [{fid:3d}] {kind:30s}  {msg[:100]}")

    # Criterio de cierre de la rebanada 1: cobertura ≥ 95% de round-trip exacto
    coverage = roundtrip_match / total if total else 0.0
    assert coverage >= 0.95, (
        f"cobertura insuficiente: {coverage:.0%} (objetivo ≥ 95%); "
        f"{len(failures)} fallos sobre {total}"
    )
