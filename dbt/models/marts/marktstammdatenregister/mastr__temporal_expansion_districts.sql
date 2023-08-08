with storage as (
    select
        district_id,
        district,
        installation_year,
        storage_capacity,
        download_date
    from {{ ref('stg_mastr__storages') }}
),

biomass as (
    select
        district_id,
        district,
        installation_year,
        power,
        download_date
    from {{ ref('stg_mastr__biomass') }}
),

solar as (
    select
        district_id,
        district,
        installation_year,
        power,
        download_date
    from {{ ref('stg_mastr__solar') }}
),

wind as (
    select
        district_id,
        district,
        installation_year,
        power,
        download_date
    from {{ ref('stg_mastr__wind') }}
),

stg_districts as (
    select
        district_id,
        district
    from {{ ref('stg_districts__area') }}
),

years as (
    select cast(generate_series(1995, 2023) as varchar(4)) as installation_year
    union all
    select null as installation_year
),


districts_with_years as (
    select
        stg_districts.district_id,
        stg_districts.district,
        years.installation_year
    from stg_districts
    cross join years
),



storage_aggregated as (

    select
        district_id,
        installation_year,
        max(download_date) as download_date_storage,
        sum(storage_capacity) as storage_capacity,
        max(district) as district
    from storage
    group by district_id, installation_year
    order by district_id, installation_year
),

storage_cummulative as (
    select
        district_id,
        installation_year,
        district,
        download_date_storage,
        storage_capacity as capacity_storage_per_year,
        sum(storage_capacity)
            over (
                partition by district_id order by district_id, installation_year
            )
        as capacity_storage_cummulative
    from storage_aggregated
),

biomass_aggregated as (

    select
        district_id,
        installation_year,
        max(download_date) as download_date_biomass,
        sum(power) as power,
        max(district) as district
    from biomass
    group by district_id, installation_year
    order by district_id, installation_year
),

biomass_cummulative as (
    select
        district_id,
        installation_year,
        district,
        download_date_biomass,
        power as power_biomass_per_year,
        sum(power)
            over (
                partition by district_id order by district_id, installation_year
            )
        as power_biomass_cummulative
    from biomass_aggregated
),

solar_aggregated as (

    select
        district_id,
        installation_year,
        max(download_date) as download_date_solar,
        sum(power) as power,
        max(district) as district
    from solar
    group by district_id, installation_year
    order by district_id, installation_year
),

solar_cummulative as (
    select
        district,
        district_id,
        installation_year,
        download_date_solar,
        power as power_solar_per_year,
        sum(power)
            over (
                partition by district_id order by district_id, installation_year
            )
        as power_solar_cummulative
    from solar_aggregated
),

wind_aggregated as (

    select
        district_id,
        installation_year,
        max(download_date) as download_date_wind,
        sum(power) as power,
        max(district) as district
    from wind
    group by district_id, installation_year
    order by district_id, installation_year
),

wind_cummulative as (
    select
        district_id,
        district,
        installation_year,
        download_date_wind,
        power as power_wind_per_year,
        sum(power)
            over (
                partition by district_id order by district_id, installation_year
            )
        as power_wind_cummulative
    from wind_aggregated
),

final as (
    select
        d.district_id,
        d.district,
        d.installation_year,
        b.power_biomass_per_year,
        b.power_biomass_cummulative,
        b.download_date_biomass,
        so.power_solar_per_year,
        so.power_solar_cummulative,
        so.download_date_solar,
        w.power_wind_per_year,
        w.power_wind_cummulative,
        w.download_date_wind,
        st.capacity_storage_per_year,
        st.capacity_storage_cummulative,
        st.download_date_storage
    from districts_with_years as d
    left join biomass_cummulative as b
        on
            d.district_id
            = b.district_id
            and d.installation_year
            = b.installation_year
    left join solar_cummulative as so
        on
            d.district_id
            = so.district_id
            and d.installation_year
            = so.installation_year
    left join wind_cummulative as w
        on
            d.district_id
            = w.district_id
            and d.installation_year
            = w.installation_year
    left join storage_cummulative as st on
        d.district_id
        = st.district_id
        and d.installation_year
        = st.installation_year
)

select * from final
