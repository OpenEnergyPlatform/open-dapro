{% test expect_column_pair_division_to_be_within(model, column_A, column_B, min_value, max_value) %}

    WITH no_zeros_in_column_b AS (
        SELECT
        *
        FROM
            {{ model }}
        WHERE
            -- Avoid dividing by zero
            {{ column_B }} != 0
    )
    SELECT * FROM no_zeros_in_column_b
        WHERE
        ({{ column_A }} / {{ column_B }}) <= {{ min_value }}
        OR ({{ column_A }} / {{ column_B }}) >= {{ max_value }}
{% endtest %}