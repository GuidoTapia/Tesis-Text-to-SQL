# tesis-sqlpgq

Implementación experimental de la tesis *Text-to-SQL/PGQ con representación intermedia unificada y verificación estructurada*.

Copyright (c) 2026 Guido Tapia. Código licenciado bajo Apache-2.0 (ver [LICENSE](LICENSE)).
El texto de la tesis (carpeta `../Arquitectura Text-to-SQL/`) no está cubierto por esta licencia.

## Estructura

```
core/           Contribución propia (IR, compilador, verificador, feedback)
baselines/      Sistemas de referencia (XiYan-SQL, CHESS) como submódulos git
execution/      Integración con el motor DuckDB + DuckPGQ
evaluation/     Marco de evaluación (NL2SQL360 y métricas propias de PGQ)
corpus/         Conjuntos de datos (Spider/BIRD, corpus PGQ sintético)
docs/           Documentación técnica y lab notebook
tests/          Tests unitarios e integración
notebooks/      Exploración y análisis reproducible
```

## Requisitos del entorno

- Python 3.11 (gestionado por [uv](https://docs.astral.sh/uv/))
- DuckDB CLI v1.4.4 (ver [DECISIONS.md](DECISIONS.md) para motivo del pin)
- Extensión `duckpgq` (instalable desde el community repo)

## Setup inicial

```bash
cd Implementaciones
uv sync                          # crea .venv e instala dependencias
source .venv/bin/activate        # activa el entorno
pytest                           # corre los tests
```

## Decisiones técnicas

Todas las decisiones fundacionales están en [DECISIONS.md](DECISIONS.md).
Las notas de experimentación y problemas encontrados están en [docs/lab_notebook.md](docs/lab_notebook.md).
