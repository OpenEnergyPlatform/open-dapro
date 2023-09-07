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
)

SELECT * FROM source_lod2
