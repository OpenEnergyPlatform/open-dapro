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

final AS (
    SELECT
        gml_id AS alkis_id,
        aktualit AS updated_at,
        nutzart AS usage_type,
        bez AS usage_type_specification,
        geometry
    FROM source_alkis
)

SELECT * FROM final
