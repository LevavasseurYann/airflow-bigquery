{#
  non_negative — a custom GENERIC test.

  Generic tests are reusable assertions referenced by name in _schema.yml:

      columns:
        - name: salary
          tests:
            - non_negative

  Like every dbt test, it returns the FAILING rows; zero rows means pass.
#}
{% test non_negative(model, column_name) %}

select *
from {{ model }}
where {{ column_name }} < 0

{% endtest %}
