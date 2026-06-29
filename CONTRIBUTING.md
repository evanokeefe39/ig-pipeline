# Contributing

Issues and PRs welcome. Before submitting:

## Setup
```bash
uv venv
uv pip install -e ".[dev]"
pre-commit install
```

## Tests
```bash
pytest tests/ -v
```

Tests use in-memory DuckDB (`:memory:`) via dependency injection. Add a test for any new pipeline function:
```python
def test_my_function():
    db = get_db(":memory:")
    result = my_function(db=db)
    assert result.expected
```

## Linting
```bash
ruff check src/ tests/
ruff format --check src/ tests/
```

## Architecture
Read `AGENTS.md` for the layer contract and design decisions. New pipeline functions follow the dependency injection pattern (`db=None` parameter).
