{#
  generate_schema_name — overrides dbt's default schema-naming behaviour.

  dbt's built-in macro prefixes custom schemas with the target schema
  (e.g. `+schema: marts` → `main_marts`). Here we use the custom schema name
  *directly* (`marts`, `staging`, `seed`, `snapshots`).

  Why this is safe in this project: the dev and prod targets are entirely
  separate warehouses (a DuckDB file vs. a BigQuery project), so there is no
  risk of dev and prod writing to the same physical schema.

  See docs/adr/0004-duckdb-local-bigquery-production.md.
#}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
