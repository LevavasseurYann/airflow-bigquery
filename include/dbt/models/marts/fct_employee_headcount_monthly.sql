-- fct_employee_headcount_monthly — aggregated monthly active headcount.
--
-- A pre-aggregated fact table: distinct active employees by hire month and
-- department. Cheap for dashboards to query directly.

{{ config(materialized='table') }}

with active as (

    select
        {{ dbt.date_trunc('month', 'hire_date') }} as hired_month,
        department,
        employee_id
    from {{ ref('fct_employees_active') }}

)

select
    cast(hired_month as date)         as hired_month,
    department,
    count(distinct employee_id)       as headcount,
    {{ dbt.current_timestamp() }}     as dbt_loaded_at
from active
group by 1, 2
