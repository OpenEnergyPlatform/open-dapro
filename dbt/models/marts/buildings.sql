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
    FROM osm
    LEFT JOIN alkis ON ST_CONTAINS(alkis.geometry, osm.geometry)
    LEFT JOIN solar ON ST_CONTAINS(osm.geometry, solar.coordinate)
    LEFT JOIN storages ON ST_CONTAINS(osm.geometry, storages.coordinate)
    LEFT JOIN lod2 ON ST_INTERSECTS(lod2.geometry, osm.geometry)
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
        CASE
            WHEN intersection_over_union > 0.7 THEN building_volume
            ELSE NULL  -- or 'None' if you prefer a string
        END AS building_volume,
        CASE
            WHEN intersection_over_union > 0.7 THEN roof_area_north
            ELSE NULL  -- or 'None' if you prefer a string
        END AS roof_area_north,
        CASE
            WHEN intersection_over_union > 0.7 THEN roof_area_east
            ELSE NULL  -- or 'None' if you prefer a string
        END AS roof_area_east,
        CASE
            WHEN intersection_over_union > 0.7 THEN roof_area_south
            ELSE NULL  -- or 'None' if you prefer a string
        END AS roof_area_south,
        CASE
            WHEN intersection_over_union > 0.7 THEN roof_area_west
            ELSE NULL  -- or 'None' if you prefer a string
        END AS roof_area_west,
        CASE
            WHEN intersection_over_union > 0.7 THEN roof_area_undefined
            ELSE NULL  -- or 'None' if you prefer a string
        END AS roof_area_undefined,
        alkis_type,
        alkis_type_specification,
        osm_type,
        solar_power,
        storage_capacity,
        mastr_updated_at,
        lod2_geometry,
        geometry
    FROM table_with_iou
)

SELECT * FROM final
