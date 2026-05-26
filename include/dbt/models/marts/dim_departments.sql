-- dim_departments — the department dimension.
--
-- One row per department, enriched with the seeded planning targets. Built as a
-- table: pre-computed and fast for the BI tools and quality checks downstream.

{{ config(materialized='table') }}

with departments as (

    select distinct department
    from {{ ref('stg_employees') }}

),

targets as (

    select
        lower(trim(department)) as department,
        target_avg_salary,
        target_headcount
    from {{ ref('department_targets') }}

)

select
    {{ surrogate_key(['departments.department']) }} as department_sk,
    departments.department,
    targets.target_avg_salary,
    targets.target_headcount,
    {{ dbt.current_timestamp() }} as dbt_loaded_at
from departments
left join targets on departments.department = targets.department
