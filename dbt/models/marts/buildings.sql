WITH alkis AS (
    SELECT * FROM {{ ref('stg_alkis') }}
),

lod2 AS (
    SELECT * FROM {{ ref('stg_lod2') }}
),

final AS (
    SELECT
        lod2.building_id AS lod2_id,
        lod2.municipality_key,
        lod2.building_volume,
        lod2.building_roof_area,
        alkis.usage_type AS alkis_type,
        alkis.usage_type_specification AS alkis_type_specification,
        lod2.geometry AS geometry --noqa: RF04
    FROM lod2
    LEFT JOIN alkis ON ST_INTERSECTS(lod2.geometry, alkis.geometry)
)

SELECT * FROM final
