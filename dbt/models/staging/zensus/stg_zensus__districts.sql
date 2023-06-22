with source as (
    select * from {{ ref('base_zensus') }}
)

select
    community_name,
    number_inhabitants,
    federal_state_in_ags || nuts2_in_ags || district_in_ags as district_id
from source
where regional_level = 'Stadtkreis/kreisfreie Stadt/Landkreis'
