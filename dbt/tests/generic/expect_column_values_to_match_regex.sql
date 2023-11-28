{% test expect_column_values_to_match_regex(model, column_name, regex) %}

SELECT 
    *
FROM
    {{ model }}
WHERE {{ column_name }} !~ '{{ regex }}' -- quotation marks are needed to get valid sql query

{% endtest %}