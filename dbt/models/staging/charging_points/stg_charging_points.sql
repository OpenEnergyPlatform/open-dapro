with source as (
    select * from {{ source('raw', 'charging_points') }}
),

final as (
    select
        "Postleitzahl" as zip_code,
        "Ort" as municipality,
        "Straße" as street,
        "Hausnummer" as street_number,
        "Adresszusatz" as adress_addition,
        "Art der Ladeeinrichung" as charging_equipment,
        "Anzahl Ladepunkte" as amount_charging_points,
        "Längengrad" as longitude,
        "Breitengrad" as latitude,
        COALESCE("Steckertypen4" || ' - ' || "P4 [kW]" || ' kW | ', '')
        || COALESCE("Steckertypen3" || ' - ' || "P3 [kW]" || ' kW | ', '')
        || COALESCE("Steckertypen2" || ' - ' || "P2 [kW]" || ' kW | ', '')
        || COALESCE(
            "Steckertypen1" || ' - ' || "P1 [kW]" || ' kW', ''
        ) as connectors_and_power,
        ST_MAKEPOINT(
            CAST(REPLACE(REPLACE("Längengrad", ',', '.'), ' ', '') AS float),
            CAST(REPLACE(REPLACE("Breitengrad", ',', '.'), ' ', '') AS float),
            4326
        ) as geo_point 
        -- The replacement of the empty space here is actually a special char - not the empty space
        -- You need to leave it that way

    from source
)

select * from final
