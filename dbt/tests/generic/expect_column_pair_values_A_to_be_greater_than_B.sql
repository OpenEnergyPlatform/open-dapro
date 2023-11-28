{% test expect_column_pair_values_A_to_be_greater_than_B(model, column_A, column_B, or_equal=False) %}

SELECT 
    *
FROM
    {{ model }}
WHERE 
    {% if or_equal %}
            {{ column_B }} > {{ column_A }}
    {% else %}
        {{ column_B }} >= {{ column_A }}
    {% endif %}

{% endtest %}