with mastr_temporal_expansion as (
    select
        municipality_id,
        power_biomass_per_year,
        power_solar_per_year,
        power_wind_per_year,
        capacity_storage_per_year,
        download_date_biomass,
        download_date_solar,
        download_date_wind,
        download_date_storage
    from {{ ref('mastr__temporal_expansion_municipalities') }}
),

municipalities as (
    select
        municipality_id,
        municipality,
        number_inhabitants,
        geometry_array,
        center
    from {{ ref('int_municipalities') }}
),

charging_points_per_municipality as (
    select * from {{ ref('int_charging_points__municipalities') }}
),

mastr_aggregated as (
    select
        municipality_id,
        max(download_date_biomass) as download_date_biomass,
        max(download_date_solar) as download_date_solar,
        max(download_date_wind) as download_date_wind,
        max(download_date_storage) as download_date_storage,
        sum(power_biomass_per_year) as power_biomass,
        sum(power_solar_per_year) as power_solar,
        sum(power_wind_per_year) as power_wind,
        sum(capacity_storage_per_year) as capacity_storage
    from mastr_temporal_expansion
    group by municipality_id
),

join_municipalities as (
    select
        municipalities.municipality_id,
        municipalities.municipality,
        municipalities.number_inhabitants,
        charging_points_per_municipality.amount_charging_points,
        municipalities.geometry_array,
        municipalities.center
    from municipalities
    left join charging_points_per_municipality
        on municipalities.municipality_id = charging_points_per_municipality.municipality_id
),

final as (
    select --noqa: ST06
        join_municipalities.municipality_id,
        join_municipalities.municipality,
        join_municipalities.number_inhabitants,
        join_municipalities.amount_charging_points,
        mastr_aggregated.download_date_biomass,
        mastr_aggregated.download_date_solar,
        mastr_aggregated.download_date_wind,
        mastr_aggregated.download_date_storage,
        round(mastr_aggregated.power_biomass) as power_biomass,
        round(mastr_aggregated.power_solar) as power_solar,
        round(mastr_aggregated.power_wind) as power_wind,
        round(mastr_aggregated.capacity_storage) as capacity_storage,
        join_municipalities.geometry_array,
        join_municipalities.center
    from join_municipalities
    left join
        mastr_aggregated
        on join_municipalities.municipality_id = mastr_aggregated.municipality_id
)

select * from final
