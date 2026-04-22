# DECISIONS.md

Registro de decisiones técnicas fundacionales del proyecto de implementación de la tesis.
Cada entrada tiene: fecha, decisión, justificación, y alternativas descartadas.
No se modifican entradas existentes — las decisiones revertidas se agregan como nuevas entradas que anulan las previas.

---

## 2026-04-22 — Ubicación del proyecto de código

**Decisión.** El código vive en `Implementaciones/` dentro del repositorio `Tesis-Text-to-SQL/` (mismo repo git que la tesis LaTeX).

**Justificación.** Simplifica la gestión de un único repositorio para el proyecto de tesis completo (texto + código). Los submódulos externos se anidarán en `Implementaciones/baselines/`, `Implementaciones/corpus/`, etc.

**Consecuencias.**
- La licencia Apache-2.0 se coloca en `Implementaciones/LICENSE` (aplica solo al código; la tesis tiene su propia licencia implícita).
- El `.gitignore` del código se agrega al existente en la raíz, no lo reemplaza.
- `git submodule status` mostrará paths con prefijo `Implementaciones/`.

**Alternativas descartadas.**
- Repo separado `tesis-sqlpgq/` fuera de la tesis: más limpio pero duplica gestión de versiones y complica la trazabilidad entre capítulos y código.

---

## 2026-04-22 — Gestor de entornos Python

**Decisión.** `uv` (Astral) con Python 3.11.15, en lugar de conda.

**Justificación.** Los tres repos externos (XiYan-SQL, CHESS, CypherBench) son Python puro — no requieren paquetes no-Python que justifiquen conda. `uv` es 10–100× más rápido que conda/pip, gestiona Python (reemplaza pyenv), y produce lockfiles reproducibles (`uv.lock`).

**Instalado vía.** `brew install uv` (versión 0.11.7). El instalador oficial `curl https://astral.sh/uv/install.sh` falló por timeout de red contra `releases.astral.sh`.

**Python.** `uv python install 3.11` → Python 3.11.15 en `~/.local/share/uv/python/`.

**Alternativas descartadas.**
- `conda`: el plan original lo recomendaba por "compatibilidad con los tres repos", pero ninguno de los tres requiere dependencias de conda específicamente.
- `pyenv + venv`: flujo clásico pero más fragmentado y lento.
- `poetry`: más lento que uv, menor adopción en 2026.

**Escape hatch.** Si en Fase 2 algún submódulo exige explícitamente un env de conda, se crea uno conda solo para ese subsistema. `uv` y `conda` coexisten sin conflicto.

---

## 2026-04-22 — Motor de ejecución: DuckDB pinned a v1.4.4

**Decisión.** DuckDB (CLI y Python bindings) pinned a **v1.4.4**, no la última stable (1.5.2).

**Justificación crítica.** La extensión community `duckpgq` NO está publicada para DuckDB 1.5.2 en `osx_arm64` (HTTP 404). Dado que SQL/PGQ vía DuckPGQ es el núcleo experimental de la tesis, no se puede avanzar con una versión de DuckDB donde la extensión no carga.

**Versiones probadas con DuckPGQ disponible (osx_arm64):**
- v1.4.4 ✓ (elegida — última compatible, alineada CLI + Python)
- v1.4.3 ✓
- v1.4.1 ✓
- v1.3.2 ✓
- v1.2.2, v1.2.1, v1.2.0 ✓
- v1.4.2 ✗ (sin extensión)
- v1.4.0 ✗ (sin extensión)

**Binarios instalados.**
- `~/.duckdb/cli/1.4.4/duckdb` ← versión canónica del proyecto
- `~/.duckdb/cli/1.4.1/duckdb` ← instalación intermedia (puede borrarse)
- `~/.duckdb/cli/1.5.2/duckdb` ← instalada por default; queda para comparaciones puntuales
- `~/.local/bin/duckdb` es un symlink a `1.5.2/duckdb` (creado por el instalador oficial). **No usar** para código del proyecto; invocar `1.4.4/duckdb` explícitamente o repointear el symlink.

**Python bindings.** `pyproject.toml` declara `duckdb>=1.4.1,<1.5`; uv resuelve a `duckdb==1.4.4` (verificado compatible con la extensión `duckpgq`).

**Verificación exitosa (Paso 0.2 del plan).** La consulta de ejemplo devuelve `Alice | Bob` tanto con el CLI como desde Python.

**Seguimiento.** Cuando DuckPGQ publique extensión para 1.5.x, evaluar migración. La lógica SQL/PGQ del estándar no debería cambiar, pero sí hay que re-probar la sintaxis.

---

## 2026-04-22 — Sintaxis PGQ: variables obligatorias en edges

**Decisión operativa.** Todos los patrones `MATCH` de SQL/PGQ deben declarar variable explícita en el edge.

**Ejemplo.**
- ❌ `MATCH (a:Person)-[:knows]->(b:Person)` — falla con "All patterns must bind to a variable, knows is missing a variable"
- ✓ `MATCH (a:Person)-[k:knows]->(b:Person)`

**Alcance.** Aplica a DuckPGQ sobre DuckDB v1.4.1. Puede relajarse en versiones posteriores. La IR-SQL/PGQ del proyecto debe generar edges con variable siempre, aunque la variable no se use en `COLUMNS`.

---

## 2026-04-22 — Proveedor inicial de LLM

**Decisión.** Anthropic Claude, modelo inicial **Claude Haiku 4.5** (`claude-haiku-4-5-20251001`) para desarrollo y validación de pipeline.

**Justificación.** Credenciales de Anthropic ya activas. Haiku 4.5 es rápido y económico — suficiente para validar la arquitectura end-to-end en Fase 3 sin quemar presupuesto.

**Escalamiento previsto.**
- Fase 3 (validación de pipeline): Haiku 4.5.
- Fase 5 (experimentos reportables): evaluar si Sonnet 4.6 (`claude-sonnet-4-6`) mejora métricas de forma significativa antes de fijarlo como modelo definitivo.
- Opus 4.7 solo si algún subconjunto de preguntas difíciles justifica el costo.

**Alternativas descartadas (por ahora).**
- OpenAI GPT-4o-mini: viable pero requiere configurar credenciales aparte.
- Modelos locales (QwenCoder, Llama-SQL): fase posterior, cuando la arquitectura esté validada y se quiera controlar costo/latencia a escala.

---

## 2026-04-22 — Sistema operativo de desarrollo

**Decisión.** macOS (Darwin 24.2.0, arm64).

**Justificación.** Es la máquina de trabajo actual. DuckPGQ tiene binarios para `osx_arm64` (verificado). XiYan-SQL, CHESS y CypherBench son compatibles con Unix.

**Riesgos conocidos.** Si algún submódulo asume Linux explícitamente (paths, variables, scripts `.sh` con utilidades GNU específicas), se documentará en su momento. Plan de mitigación: Docker o devcontainer en caso de incompatibilidad bloqueante.
