{{ 
    config(
        materialized='table',
        indexes=[
            {'columns': ['geometry'], 'type': 'gist'}
        ]
    ) 
}}
WITH source_alkis AS (
    SELECT * FROM {{ source('raw', 'load_alkis') }}
),

selected_rows AS (
    SELECT
        gml_id AS alkis_id,
        aktualit AS updated_at,
        nutzart AS usage_type,
        bez AS usage_type_specification,
        geometry
    FROM source_alkis
    WHERE nutzart IN (
        'Fläche besonderer funktionaler Prägung',
        'Fläche gemischter Nutzung',
        'Flugverkehr',
        'Industrie- und Gewerbefläche',
        'Landwirtschaft',
        'Wohnbaufläche'
    )
),

final AS (
    SELECT
        alkis_id,
        updated_at,
        REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(usage_type, 'ä', 'ae'), 'ö', 'oe'), 'ü', 'ue'), 'ß', 'ss'), 'Ä', 'Ae'), 'Ü', 'Ue'), 'Ö', 'Oe') AS usage_type,
        REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(usage_type_specification, 'ä', 'ae'), 'ö', 'oe'), 'ü', 'ue'), 'ß', 'ss'), 'Ä', 'Ae'), 'Ü', 'Ue'), 'Ö', 'Oe') AS usage_type_specification,
        geometry
    FROM
        selected_rows
)

SELECT * FROM final
