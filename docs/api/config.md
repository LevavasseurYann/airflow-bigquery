# config

Environment-driven configuration. A single immutable `Settings` object resolves
every path, schema name and credential from environment variables. The same DAG
code runs unchanged in both targets — only the environment differs.

| `HR_ENV` | Warehouse | Credentials |
|----------|-----------|-------------|
| `local` (default) | DuckDB file | none |
| `production` | Google BigQuery | GCP service account |

---

::: hr_pipeline.config.Environment

---

::: hr_pipeline.config.Settings
