# Contributing

This is a personal demonstration project, but it is run with the conventions of
a team repository — that consistency is part of what it demonstrates.

## Local setup

```bash
python -m venv .venv && source .venv/bin/activate   # Windows: .venv\Scripts\activate
pip install -e ".[dev]"
pre-commit install
```

You do **not** need Docker for the unit tests or for linting — only to run the
full Airflow cluster.

## Workflow

1. Branch from `main` (`feat/...`, `fix/...`, `docs/...`).
2. Make the change. Keep orchestration in `dags/`, logic in `hr_pipeline`.
3. Run the checks below.
4. Open a PR using the template; CI must be green.

## Quality gates

| Check | Command |
|-------|---------|
| Lint | `ruff check src dags tests scripts` |
| Format | `ruff format src dags tests scripts` |
| Types | `mypy` |
| Unit tests | `pytest -m "not dags"` |
| DAG validation | `pytest -m dags` |
| dbt | `dbt build --project-dir include/dbt --profiles-dir include/dbt` |

`pre-commit` runs the lint/format gates automatically on every commit.

## Conventions

- **DAGs** must be tagged, described and owned — the cluster policy enforces it.
- **Business logic** belongs in `hr_pipeline` (typed, unit-tested), never inline
  in a DAG file.
- **dbt SQL** stays portable — use dbt cross-database macros, not warehouse-
  specific functions.
- **Decisions** that change the architecture get an ADR in `docs/adr/`.
- **Commits** follow [Conventional Commits](https://www.conventionalcommits.org/)
  (`feat:`, `fix:`, `docs:`, `refactor:`, `test:`, `chore:`).
