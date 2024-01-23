with storage as (
    select
        municipality_id,
        installation_year,
        storage_capacity,
        download_date
    from {{ ref('stg_mastr__storages') }}
    where operating_status = 'In Betrieb'
),

biomass as (
    select
        municipality_id,
        installation_year,
        power,
        download_date
    from {{ ref('stg_mastr__biomass') }}
    where operating_status = 'In Betrieb'
),

solar as (
    select
        municipality_id,
        installation_year,
        power_net as power,
        download_date
    from {{ ref('stg_mastr__solar') }}
    where operating_status = 'In Betrieb'
),

wind as (
    select
        municipality_id,
        installation_year,
        power,
        download_date
    from {{ ref('stg_mastr__wind') }}
    where operating_status = 'In Betrieb'
),

municipalities as (
    select 
        municipality_id,
        municipality
    from {{ ref('stg_destatis_areas_and_inhabitants') }}
),

years as (
    select cast(generate_series(1995, 2023) as int) as installation_year
    union all
    select null as installation_year
),

municipalities_with_years as (
    select
        municipalities.municipality_id,
        municipalities.municipality,
        years.installation_year
    from municipalities
    cross join years
),

storage_aggregated as (

    select
        municipality_id,
        installation_year,
        max(download_date) as download_date_storage,
        sum(storage_capacity) as storage_capacity
    from storage
    group by municipality_id, installation_year
    order by municipality_id, installation_year
),

storage_cummulative as (
    select
        municipality_id,
        installation_year,
        download_date_storage,
        storage_capacity as capacity_storage_per_year,
        sum(storage_capacity)
            over (
                partition by municipality_id
                order by municipality_id, installation_year
            )
        as capacity_storage_cummulative
    from storage_aggregated
),

storage_updated as (
    select
        municipality_id,
        installation_year,
        download_date_storage,
        capacity_storage_per_year,
        capacity_storage_cummulative
    from storage_cummulative
),

biomass_aggregated as (

    select
        municipality_id,
        installation_year,
        max(download_date) as download_date_biomass,
        sum(power) as power
    from biomass
    group by municipality_id, installation_year
    order by municipality_id, installation_year
),

biomass_cummulative as (
    select
        municipality_id,
        installation_year,
        download_date_biomass,
        power as power_biomass_per_year,
        sum(power)
            over (
                partition by municipality_id
                order by municipality_id, installation_year
            )
        as power_biomass_cummulative
    from biomass_aggregated
),

solar_aggregated as (

    select
        municipality_id,
        installation_year,
        max(download_date) as download_date_solar,
        sum(power) as power
    from solar
    group by municipality_id, installation_year
    order by municipality_id, installation_year
),

solar_cummulative as (
    select
        municipality_id,
        installation_year,
        download_date_solar,
        power as power_solar_per_year,
        sum(power)
            over (
                partition by municipality_id
                order by municipality_id, installation_year
            )
        as power_solar_cummulative
    from solar_aggregated
),

wind_aggregated as (

    select
        municipality_id,
        installation_year,
        max(download_date) as download_date_wind,
        sum(power) as power
    from wind
    group by municipality_id, installation_year
    order by municipality_id, installation_year
),

wind_cummulative as (
    select
        municipality_id,
        installation_year,
        download_date_wind,
        power as power_wind_per_year,
        sum(power)
            over (
                partition by municipality_id
                order by municipality_id, installation_year
            )
        as power_wind_cummulative
    from wind_aggregated
),

final as (
    select
        m.municipality_id,
        m.municipality,
        m.installation_year as year,
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
    from municipalities_with_years as m
    left join biomass_cummulative as b
        on
            m.municipality_id
            = b.municipality_id
            and m.installation_year
            = b.installation_year
    left join solar_cummulative as so
        on
            m.municipality_id
            = so.municipality_id
            and m.installation_year
            = so.installation_year
    left join wind_cummulative as w
        on
            m.municipality_id
            = w.municipality_id
            and m.installation_year
            = w.installation_year
    left join storage_cummulative as st on
        m.municipality_id
        = st.municipality_id
        and m.installation_year
        = st.installation_year
)

select * from final
