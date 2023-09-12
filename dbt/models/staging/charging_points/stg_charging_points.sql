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
        COALESCE("Steckertypen4" || ' - ' || "P4 [kW]" || ' kW | ', '')
        || COALESCE("Steckertypen3" || ' - ' || "P3 [kW]" || ' kW | ', '')
        || COALESCE("Steckertypen2" || ' - ' || "P2 [kW]" || ' kW | ', '')
        || COALESCE(
            "Steckertypen1" || ' - ' || "P1 [kW]" || ' kW', ''
        ) as connectors_and_power,
        "Längengrad" as longitude,
        "Breitengrad" as latitude,
        ST_SETSRID(ST_MAKEPOINT("Längengrad","Breitengrad"), 4326) as geo_point

    from source
)

select * from final
