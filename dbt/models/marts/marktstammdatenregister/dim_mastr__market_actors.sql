with source_market_actors as (
    select * from {{ ref('stg_mastr__market_actors') }}
),

source_wind as (
    select * from {{ ref('stg_mastr__wind') }}
),

source_solar as (
    select * from {{ ref('stg_mastr__solar') }}
),

source_biomass as (
    select * from {{ ref('stg_mastr__biomass') }}
),

wind_aggregated as (
    select
        unit_owner_mastr_id,
        sum(power) as wind_power_aggregated
    from source_wind
    group by unit_owner_mastr_id
),

solar_aggregated as (
    select
        unit_owner_mastr_id,
        sum(power) as solar_power_aggregated
    from source_solar
    group by unit_owner_mastr_id
),

biomass_aggregated as (
    select
        unit_owner_mastr_id,
        sum(power) as biomass_power_aggregated
    from source_biomass
    group by unit_owner_mastr_id
),

renamed_market_actors as (
    select
        *,
        "mastr_id" as unit_owner_mastr_id
    from source_market_actors
),

market_actors_aggregated as (
    select
        m.*,
        b.biomass_power_aggregated,
        s.solar_power_aggregated,
        w.wind_power_aggregated
    from renamed_market_actors as m
    left join
        biomass_aggregated as b
        on (m.unit_owner_mastr_id = b.unit_owner_mastr_id)
    left join
        solar_aggregated as s
        on (m.unit_owner_mastr_id = s.unit_owner_mastr_id)
    left join
        wind_aggregated as w
        on (m.unit_owner_mastr_id = w.unit_owner_mastr_id)
)

select * from market_actors_aggregated
