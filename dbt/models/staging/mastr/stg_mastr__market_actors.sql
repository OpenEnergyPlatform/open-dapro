with source as (
    select * from {{ source('raw_mastr', 'market_actors') }}
),

renamed as (

    select
        "MastrNummer" as mastr_id,
        "Personenart" as actor_type,
        "Marktfunktion" as market_function,
        "Firmenname" as company_name,
        "Rechtsform" as legal_status,
        "Land" as country,
        "Strasse" as street,
        "Hausnummer" as street_number,
        "Postleitzahl" as zip_code,
        "Ort" as municipality,
        "Bundesland" as state,
        "Taetigkeitsbeginn" as start_of_activities,
        "Webseite" as website,
        "DatumDownload" as download_date
    from source

)

select * from renamed
