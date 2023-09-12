with source as (
    select * from {{ source('raw', 'destatis_areas_and_inhabitants') }}
),

municipalities_filtered as (
    select
        LPAD(source."01"::text, 2, '0') as federal_state,
        source."Unnamed: 3"::text as nuts,
        LPAD(source."Unnamed: 4"::text, 2, '0') as d_id,
        LPAD(source."Unnamed: 6"::text, 3, '0') as m_id,
        source."Schleswig-Holstein" as municipality,
        source."Unnamed: 10" as number_inhabitants
    from source
    where source."Unnamed: 6" is not null and source."Unnamed: 4" is not null
),

municipalities as (
  select
      municipality,
      federal_state || nuts || d_id || m_id as municipality_id,
      federal_state || nuts || d_id as district_id,
      number_inhabitants
  from municipalities_filtered
),

districts_filtered as (
    select
        LPAD(source."01"::text, 2, '0') as federal_state,
        source."Unnamed: 3"::text as nuts,
        LPAD(source."Unnamed: 4"::text, 2, '0') as d_id,
        source."Schleswig-Holstein" as district
    from source
    where source."Unnamed: 4" is not null and source."Unnamed: 5" is null
),

districts as (
    select
        district,
        federal_state || nuts || d_id as district_id
    from districts_filtered
),

final as (
    select
        m.municipality,
        m.municipality_id,
        d.district,
        d.district_id,
        m.number_inhabitants
    from municipalities as m left join districts as d on m.district_id = d.district_id
)

select * from final