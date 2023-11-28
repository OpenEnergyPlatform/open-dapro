{% test expect_column_values_to_be_between(model, column_name, min_value, max_value, row_condition) %}

WITH selection AS (
    SELECT *
    FROM
        {{ model }}
    {% if row_condition %}
    WHERE
        {{ row_condition }}
    {% endif %}
)
 
SELECT *
FROM selection
WHERE {{ column_name }} < {{ min_value }} OR {{ column_name }} > {{ max_value }}

{% endtest %}