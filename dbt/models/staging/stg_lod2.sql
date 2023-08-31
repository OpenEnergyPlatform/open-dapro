WITH source_lod2 AS (
    SELECT * FROM {{ source('raw', 'building_data_from_cityjson') }}
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
