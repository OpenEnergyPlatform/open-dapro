{{ 
    config(
        indexes=[
            {'columns': ['geometry'], 'type': 'gist'}
        ]
    ) 
}}

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

combustion AS (
    SELECT * FROM {{ ref('stg_mastr__combustion') }} WHERE coordinate IS NOT NULL
),

biomass AS (
    SELECT * FROM {{ ref('stg_mastr__biomass') }} WHERE coordinate IS NOT NULL
),



joined_tables AS (
    SELECT
        lod2.building_id AS lod2_id,
        lod2.municipality_key,
        lod2.building_volume,
        lod2.building_height,
        lod2.building_envelope_area,
        lod2.roof_area_north,
        lod2.roof_area_east,
        lod2.roof_area_south,
        lod2.roof_area_west,
        lod2.roof_area_undefined,
        alkis.usage_type AS alkis_type,
        alkis.usage_type_specification AS alkis_type_specification,
        osm.type AS osm_type,
        osm.osm_id,
        solar.power AS solar_power,
        biomass.power AS biomass_power,
        combustion.power AS combustion_power,
        combustion.energy_carrier AS combustion_energy_carrier,
        storages.storage_capacity AS storage_capacity,
        solar.download_date AS mastr_updated_at,
        lod2.geometry AS lod2_geometry,
        osm.geometry AS geometry --noqa 
    FROM osm
    LEFT JOIN alkis ON ST_CONTAINS(alkis.geometry, osm.geometry)
    LEFT JOIN solar ON ST_CONTAINS(osm.geometry, solar.coordinate)
    LEFT JOIN biomass ON ST_CONTAINS(osm.geometry, biomass.coordinate)
    LEFT JOIN combustion ON ST_CONTAINS(osm.geometry, combustion.coordinate)
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

ranked_table_by_iou AS (
    SELECT
        *,
        ROW_NUMBER() OVER (Partition BY osm_id ORDER BY intersection_over_union DESC) AS row_number
    FROM table_with_iou
),

table_with_unique_id AS (
    SELECT 
        *
    FROM ranked_table_by_iou
    WHERE row_number = 1
),

final AS (
    SELECT
        municipality_key,
        CASE
            WHEN intersection_over_union > 0.7 THEN lod2_id
            ELSE NULL  
        END AS lod2_id,
        CASE
            WHEN intersection_over_union > 0.7 THEN building_volume
            ELSE NULL  
        END AS building_volume,
        CASE
            WHEN intersection_over_union > 0.7 THEN building_height
            ELSE NULL
        END AS building_height,
        CASE
            WHEN intersection_over_union > 0.7 THEN building_envelope_area
            ELSE NULL
        END AS building_envelope_area,
        CASE
            WHEN intersection_over_union > 0.7 THEN roof_area_north
            ELSE NULL
        END AS roof_area_north,
        CASE
            WHEN intersection_over_union > 0.7 THEN roof_area_east
            ELSE NULL  
        END AS roof_area_east,
        CASE
            WHEN intersection_over_union > 0.7 THEN roof_area_south
            ELSE NULL
        END AS roof_area_south,
        CASE
            WHEN intersection_over_union > 0.7 THEN roof_area_west
            ELSE NULL
        END AS roof_area_west,
        CASE
            WHEN intersection_over_union > 0.7 THEN roof_area_undefined
            ELSE NULL  
        END AS roof_area_undefined,
        alkis_type,
        alkis_type_specification,
        osm_type,
        osm_id,
        solar_power,
        storage_capacity,
        mastr_updated_at,
        lod2_geometry,
        geometry
    FROM table_with_unique_id
)

SELECT * FROM final
