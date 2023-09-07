WITH alkis AS (
    SELECT * FROM {{ ref('stg_alkis') }}
),

lod2 AS (
    SELECT * FROM {{ ref('stg_lod2') }}
),

osm AS (
    SELECT * FROM {{ ref('stg_osm') }}
),

solar AS (
    SELECT * FROM {{ ref('stg_mastr__solar') }} WHERE coordinate IS NOT NULL
),

storages AS (
    SELECT * FROM {{ ref('stg_mastr__storages') }} WHERE coordinate IS NOT NULL
),

joined_tables AS (
    SELECT
        lod2.building_id AS lod2_id,
        lod2.municipality_key,
        lod2.building_volume,
        lod2.building_roof_area,
        alkis.usage_type AS alkis_type,
        alkis.usage_type_specification AS alkis_type_specification,
        osm.type as osm_type,
        solar.power AS solar_power,
        storages.storage_capacity AS storage_capacity,
        lod2.geometry AS lod2_geometry,
        osm.geometry AS osm_geometry
    FROM lod2
    LEFT JOIN alkis ON ST_CONTAINS(alkis.geometry, lod2.geometry)
    LEFT JOIN solar ON ST_CONTAINS(lod2.geometry, solar.coordinate)
    LEFT JOIN storages ON ST_CONTAINS(lod2.geometry, storages.coordinate)
    LEFT JOIN osm ON ST_INTERSECTS(lod2.geometry, osm.geometry)
    -- ST_Area(ST_Intersection(lod2.geometry, osm.geometry), false) / (ST_Area(ST_Union(lod2.geometry, osm.geometry), false)+0.00001) > 0.7
),

table_with_iou AS (
    SELECT
        *,
        ST_Area(ST_Intersection(lod2_geometry, osm_geometry)) / ST_Area(ST_Union(lod2_geometry, osm_geometry)) AS intersection_over_union
    FROM joined_tables
),

final AS (
    SELECT
        lod2_id,
        municipality_key,
        building_volume,
        building_roof_area,
        alkis_type,
        alkis_type_specification,
        osm_type,
        solar_power,
        storage_capacity,
        lod2_geometry,
        osm_geometry
    FROM table_with_iou
    WHERE intersection_over_union >= 0.7
    UNION
    SELECT
        lod2_id,
        municipality_key,
        building_volume,
        building_roof_area,
        alkis_type,
        alkis_type_specification,
        osm_type,
        solar_power,
        storage_capacity,
        lod2_geometry,
        osm_geometry
    FROM joined_tables WHERE osm_geometry IS NULL
)

SELECT * FROM final
