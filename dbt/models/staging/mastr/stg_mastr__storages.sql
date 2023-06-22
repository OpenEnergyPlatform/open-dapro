with source_storage_extended as (
    select * from {{ ref('base_mastr__storage_extended') }}
),

source_storage_units as (
    select * from {{ ref('base_mastr__storage_units') }}
),

renamed_storage_units as (
    select
        linked_unit_id as mastr_id,
        storage_capacity
    from source_storage_units
),

storage_units as (
    select
        renamed_storage_units.mastr_id as mastr_id,
        renamed_storage_units.storage_capacity,
        source_storage_extended.municipality_id,
        source_storage_extended.district_id,
        source_storage_extended.municipality,
        source_storage_extended.district,
        source_storage_extended.zip_code,
        source_storage_extended.commissioning_date,
        source_storage_extended.planned_commissioning_date,
        source_storage_extended.installation_year,
        source_storage_extended.power,
        source_storage_extended.download_date
    from renamed_storage_units

    left join
        source_storage_extended
        on renamed_storage_units.mastr_id = source_storage_extended.mastr_id
)

select * from storage_units
