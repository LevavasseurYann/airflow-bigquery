# API Reference

Auto-generated from the `hr_pipeline` package docstrings via
[mkdocstrings](https://mkdocstrings.github.io/).

## Package structure

```
src/hr_pipeline/
├── config.py          # Settings — environment-driven configuration
├── warehouse.py       # Warehouse ABC + DuckDB / BigQuery implementations
├── ingestion.py       # extract → land (Parquet) → load
├── assets.py          # Airflow Asset definitions
├── quality/
│   ├── checks.py      # QualityCheck, CheckResult, QualityReport, run_check
│   └── suite.py       # build_marts_quality_suite — the HR checks catalogue
└── operators/
    └── data_quality.py  # DataQualityCheckOperator
```

## Design principles

**The package has no Airflow dependency** — except `operators/`, which
deliberately imports from `airflow.sdk` and is only ever imported from DAG
files.  Everything else (`config`, `warehouse`, `ingestion`, `quality`) is
plain Python, testable with `pytest` and no Airflow installation.

!!! note "Sphinx cross-references"
    Some docstrings use Sphinx notation (`:class:`, `:meth:`) from an earlier
    version of the codebase.  These render as plain text rather than hyperlinks.
    They will be migrated to Markdown progressively.
