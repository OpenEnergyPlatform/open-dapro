WITH alkis AS (
    SELECT * FROM {{ ref('stg_alkis') }}
),

lod2 AS (
    SELECT * FROM {{ ref('stg_lod2') }}
),

solar AS (
    SELECT * FROM {{ ref('stg_mastr__solar') }} WHERE coordinate IS NOT NULL
),

storages AS (
    SELECT * FROM {{ ref('stg_mastr__storages') }} WHERE coordinate IS NOT NULL
),

final AS (
    SELECT
        lod2.building_id AS lod2_id,
        lod2.municipality_key,
        lod2.building_volume,
        lod2.building_roof_area,
        alkis.usage_type AS alkis_type,
        alkis.usage_type_specification AS alkis_type_specification,
        solar.power AS solar_power,
        storages.storage_capacity AS storage_capacity,
        lod2.geometry AS geometry --noqa: RF04
    FROM lod2
    LEFT JOIN alkis ON ST_INTERSECTS(lod2.geometry, alkis.geometry)
    LEFT JOIN solar ON ST_CONTAINS(lod2.geometry, solar.coordinate)
    LEFT JOIN storages ON ST_CONTAINS(lod2.geometry, storages.coordinate)
)

SELECT * FROM final
