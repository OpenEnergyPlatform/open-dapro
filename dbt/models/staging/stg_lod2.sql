{{ 
    config(
        materialized='table',
        indexes=[
            {'columns': ['geometry'], 'type': 'gist'}
        ]
    ) 
}}

WITH source_lod2 AS (
    SELECT * FROM {{ source('raw', 'lod2_bavaria') }}
),

final AS (
    SELECT
        "id" AS building_id,
        municipality_key,
        building_volume,
        building_roof_area,
        geometry
    FROM source_lod2
)

SELECT * FROM final
