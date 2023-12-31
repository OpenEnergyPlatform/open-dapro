WITH municipalities AS (
    SELECT * FROM {{ ref("municipalities") }}
),

districts_area AS (
    SELECT
        *,
        st_centroid(geometry_array) AS center
    FROM {{ ref("stg_districts__area") }}
),

districts_aggregated AS (
    SELECT
        district,
        district_id,
        sum(number_inhabitants) AS number_inhabitants,
        sum(amount_charging_points) AS amount_charging_points,
        max(download_date_biomass) AS download_date_biomass,
        max(download_date_solar) AS download_date_solar,
        max(download_date_wind) AS download_date_wind,
        max(download_date_storage) AS download_date_storage,
        sum(power_biomass) AS power_biomass,
        sum(power_solar) AS power_solar,
        sum(power_wind) AS power_wind,
        sum(capacity_storage) AS capacity_storage
    FROM municipalities
    WHERE district_id IS NOT NULL
    GROUP BY district_id, district
),

final AS (
    SELECT
        a.district,
        a.district_id,
        a.number_inhabitants,
        a.amount_charging_points,
        a.download_date_biomass,
        a.download_date_solar,
        a.download_date_wind,
        a.download_date_storage,
        a.power_biomass,
        a.power_solar,
        a.power_wind,
        a.capacity_storage,
        e.geometry_array,
        e.center

    FROM districts_aggregated AS a LEFT JOIN districts_area AS e ON a.district_id = e.district_id
)

SELECT * FROM final
