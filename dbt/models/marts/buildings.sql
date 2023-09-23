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
        lod2.roof_area_north,
        lod2.roof_area_east,
        lod2.roof_area_south,
        lod2.roof_area_west,
        lod2.roof_area_undefined,
        alkis.usage_type AS alkis_type,
        alkis.usage_type_specification AS alkis_type_specification,
        osm.type AS osm_type,
        solar.power AS solar_power,
        storages.storage_capacity AS storage_capacity,
        solar.download_date AS mastr_updated_at,
        lod2.geometry AS lod2_geometry,
        osm.geometry AS geometry --noqa 
    FROM lod2
    LEFT JOIN alkis ON ST_CONTAINS(alkis.geometry, lod2.geometry)
    LEFT JOIN solar ON ST_CONTAINS(lod2.geometry, solar.coordinate)
    LEFT JOIN storages ON ST_CONTAINS(lod2.geometry, storages.coordinate)
    LEFT JOIN osm ON ST_INTERSECTS(lod2.geometry, osm.geometry)
),

table_with_iou AS (
    SELECT
        *,
        ST_AREA(
            ST_INTERSECTION(lod2_geometry, geometry)
        )
        / ST_AREA(
            ST_UNION(lod2_geometry, geometry)
        ) AS intersection_over_union
    FROM joined_tables
),

final AS (
    SELECT
        lod2_id,
        municipality_key,
        building_volume,
        roof_area_north,
        roof_area_east,
        roof_area_south,
        roof_area_west,
        roof_area_undefined,
        alkis_type,
        alkis_type_specification,
        osm_type,
        solar_power,
        storage_capacity,
        mastr_updated_at,
        lod2_geometry,
        geometry
    FROM table_with_iou
    WHERE intersection_over_union >= 0.7
    UNION
    SELECT
        lod2_id,
        municipality_key,
        building_volume,
        roof_area_north,
        roof_area_east,
        roof_area_south,
        roof_area_west,
        roof_area_undefined,
        alkis_type,
        alkis_type_specification,
        osm_type,
        solar_power,
        storage_capacity,
        mastr_updated_at,
        lod2_geometry,
        geometry
    FROM joined_tables WHERE geometry IS NULL
)

SELECT * FROM final
