WITH municipalities AS (
    SELECT * FROM {{ ref("municipalities") }}
),

districts AS (
    SELECT
        left(municipality_id, 5) AS district_id,
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
    GROUP BY district_id
)

SELECT * FROM districts
