-- assert_no_future_hires — a SINGULAR test (one-off business rule).
--
-- Singular tests are plain SELECTs in tests/: any row returned is a failure.
-- This one encodes a rule that is hard to express as a generic test —
-- no employee may have a hire date in the future.
--
-- Run with:  dbt test --select assert_no_future_hires

select
    employee_id,
    hire_date
from {{ ref('fct_employees_active') }}
where hire_date > current_date
