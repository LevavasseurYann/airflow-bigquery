-- int_employees_enriched — joins staged employees to departmental targets.
--
-- Intermediate layer: business logic that several marts share lives here once,
-- rather than being duplicated. Materialised as ephemeral — dbt inlines it as a
-- CTE into each consumer, so it occupies no storage.

{{ config(materialized='ephemeral') }}

with employees as (

    select * from {{ ref('stg_employees') }}

),

targets as (

    select
        lower(trim(department)) as department,
        target_avg_salary,
        target_headcount
    from {{ ref('department_targets') }}

),

enriched as (

    select
        employees.employee_sk,
        employees.employee_id,
        employees.email,
        employees.hire_date,
        employees.updated_at,
        employees.is_active,
        employees.department,
        employees.salary,
        targets.target_avg_salary,
        targets.target_headcount,

        -- Classify each salary against the department planning target.
        case
            when targets.target_avg_salary is null      then 'no_target'
            when employees.salary >= targets.target_avg_salary then 'above_target'
            else 'below_target'
        end as salary_vs_target

    from employees
    left join targets on employees.department = targets.department

)

select * from enriched
