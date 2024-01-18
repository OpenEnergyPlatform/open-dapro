WITH municipalities AS (
    SELECT * FROM {{ ref('municipalities') }}
)

nuts2 AS (
    SELECT
        left(nuts, 4) AS nuts2_id,
        sum(number_inhabitants) AS number_inhabitants,
        sum(amount_charging_points) AS amount_charging_points,
        max(download_date_biomass) AS download_date_biomass,
        max(download_date_solar) AS download_date_solar,
        max(download_date_wind) AS download_date_wind,
        max(download_date_storage) AS download_date_storage,
        sum(power_biomass) AS power_biomass,
        sum(power_solar) AS power_solar,
        sum(power_wind) AS power_wind,
        sum(capacity_storage) AS capacity_storage,
		ST_Union(geometry_array) AS geometry_array
    FROM municipalities
    GROUP BY nuts2_id
)

SELECT * FROM nuts2
