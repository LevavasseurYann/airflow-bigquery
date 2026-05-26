{#
  snp_employees — Slowly Changing Dimension (Type 2) history of employees.

  A snapshot captures point-in-time history: every time `dbt snapshot` runs, any
  changed row is closed off (dbt_valid_to is set) and a new version is opened.
  This answers "what did this employee's record look like on any past date?".

  Strategy 'timestamp': dbt detects change by watching the updated_at column.
#}
{% snapshot snp_employees %}

{{
    config(
        target_schema='snapshots',
        unique_key='employee_id',
        strategy='timestamp',
        updated_at='updated_at',
        invalidate_hard_deletes=true
    )
}}

select
    employee_id,
    email,
    department,
    salary,
    is_active,
    updated_at
from {{ ref('stg_employees') }}

{% endsnapshot %}
