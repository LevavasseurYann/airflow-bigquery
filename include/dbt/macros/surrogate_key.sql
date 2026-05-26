{#
  surrogate_key — build a deterministic hash key from one or more columns.

  A dependency-free, cross-database equivalent of
  `dbt_utils.generate_surrogate_key`. It relies only on dbt's built-in
  cross-database macros (`dbt.hash`, `dbt.type_string`), so it compiles
  identically on DuckDB and BigQuery.

  NULLs are coalesced to a sentinel so that (NULL, 'x') and ('x', NULL) never
  collide onto the same key.

  Usage:  {{ surrogate_key(['employee_id', 'email']) }}
#}
{% macro surrogate_key(field_list) -%}
    {%- set expressions = [] -%}
    {%- for field in field_list -%}
        {%- do expressions.append(
            "coalesce(cast(" ~ field ~ " as " ~ dbt.type_string() ~ "), '_dbt_null_')"
        ) -%}
    {%- endfor -%}
    {{ dbt.hash(expressions | join(" || '-' || ")) }}
{%- endmacro %}
