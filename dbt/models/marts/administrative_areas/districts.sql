with mastr_temporal_expansion as (
    select
        district_id,
        power_biomass_per_year,
        power_solar_per_year,
        power_wind_per_year,
        capacity_storage_per_year,
        download_date_biomass,
        download_date_solar,
        download_date_wind,
        download_date_storage
    from {{ ref('mastr__temporal_expansion_districts') }}
),

districts as (
    select
        district_id,
        district,
        number_inhabitants,
        geometry_array,
        center
    from {{ ref('int_districts') }}
),

charging_points_per_district as (
    select * from {{ ref('int_charging_points__districts') }}
),

mastr_aggregated as (
    select
        district_id,
        max(download_date_biomass) as download_date_biomass,
        max(download_date_solar) as download_date_solar,
        max(download_date_wind) as download_date_wind,
        max(download_date_storage) as download_date_storage,
        sum(power_biomass_per_year) as power_biomass,
        sum(power_solar_per_year) as power_solar,
        sum(power_wind_per_year) as power_wind,
        sum(capacity_storage_per_year) as capacity_storage
    from mastr_temporal_expansion
    group by district_id
),

join_districts as (
    select
        districts.district_id,
        district,
        number_inhabitants,
        amount_charging_points,
        geometry_array,
        center
    from districts
    left join charging_points_per_district
        on districts.district_id = charging_points_per_district.district_id
),

final as (
    select
        join_districts.district_id,
        join_districts.district,
        join_districts.number_inhabitants,
        join_districts.amount_charging_points,
        join_districts.geometry_array,
        join_districts.center,
        mastr_aggregated.download_date_biomass,
        mastr_aggregated.download_date_solar,
        mastr_aggregated.download_date_wind,
        mastr_aggregated.download_date_storage,
        round(mastr_aggregated.power_biomass) as power_biomass,
        round(mastr_aggregated.power_solar) as power_solar,
        round(mastr_aggregated.power_wind) as power_wind,
        round(mastr_aggregated.capacity_storage) as capacity_storage
    from join_districts
    left join
        mastr_aggregated
        on join_districts.district_id = mastr_aggregated.district_id
)

select * from final
