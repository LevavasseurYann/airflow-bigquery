-- fct_employees_active — incremental fact table of currently active employees.
--
-- Incremental materialisation processes only rows whose updated_at is newer
-- than what is already in the table, instead of rebuilding everything.
--
-- All warehouse-specific settings live in a single config() block:
--   * incremental_strategy — BigQuery uses native MERGE; DuckDB uses
--     delete+insert (the portable strategy the adapter supports).
--   * partition_by / cluster_by — BigQuery-only cost levers; `none` is
--     ignored by all other adapters so the file compiles unchanged on DuckDB.
--
-- Force a full rebuild with:  dbt run --full-refresh -s fct_employees_active

{{
    config(
        materialized='incremental',
        unique_key='employee_id',
        incremental_strategy='merge' if target.type == 'bigquery' else 'delete+insert',
        on_schema_change='sync_all_columns',
        partition_by={'field': 'hire_date', 'data_type': 'date', 'granularity': 'month'} if target.type == 'bigquery' else none,
        cluster_by=['department'] if target.type == 'bigquery' else none
    )
}}

with enriched as (

    select * from {{ ref('int_employees_enriched') }}

),

active_employees as (

    select
        employee_sk,
        employee_id,
        email,
        hire_date,
        updated_at,
        department,
        salary,
        salary_vs_target,
        target_avg_salary,
        target_headcount,

        -- Derived metric: whole years of tenure (cross-database datediff).
        {{ dbt.datediff('hire_date', 'current_date', 'year') }} as years_of_service,

        -- Audit column: when dbt last wrote this row. Used by the freshness check.
        {{ dbt.current_timestamp() }} as dbt_loaded_at

    from enriched
    -- Business filter lives in the mart, never in staging.
    where is_active = true
      and salary >= {{ var('active_employee_salary_floor', 0) }}

)

select * from active_employees

{% if is_incremental() %}

-- Incremental run: only rows strictly newer than the current high-water mark.
where updated_at > (
    select coalesce(max(updated_at), cast('1900-01-01' as timestamp))
    from {{ this }}
)

{% endif %}
