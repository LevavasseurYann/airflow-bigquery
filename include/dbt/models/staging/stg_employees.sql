-- stg_employees — staging model for the raw employees source.
--
-- Staging conventions (identical to the sibling dbt-bigquery repo):
--   * one staging model per source table (1:1);
--   * clean / rename / cast ONLY — no joins, no business logic, no filters;
--   * materialised as a view, so it always reflects the latest raw data.
--
-- Every cast uses a dbt cross-database macro (dbt.type_string, …) so the model
-- compiles unchanged on DuckDB and BigQuery.

{{ config(materialized='view') }}

with source as (

    select * from {{ source('raw_hr', 'employees') }}

),

cleaned as (

    select
        -- Surrogate key — deterministic hash of the natural key.
        {{ surrogate_key(['employee_id', 'email']) }}      as employee_sk,

        -- Keys: cast to string for consistency across HRIS exports.
        cast(employee_id as {{ dbt.type_string() }})       as employee_id,

        -- Contact: lower() + trim() so downstream joins are reliable.
        lower(trim(email))                                 as email,

        -- Dates & timestamps: make the type contract explicit.
        cast(hire_date as date)                            as hire_date,
        cast(updated_at as timestamp)                      as updated_at,

        -- Flags: the raw layer stores booleans as strings ("true"/"false").
        cast(is_active as boolean)                         as is_active,

        -- Descriptors: normalise, and bucket blanks into 'unknown'.
        coalesce(nullif(lower(trim(department)), ''), 'unknown') as department,

        -- Financials: numeric preserves exact decimal precision.
        cast(salary as {{ dbt.type_numeric() }})           as salary

    from source

)

select * from cleaned
