WITH source_oberbayern AS (
    SELECT * FROM {{ source('raw', 'osm_oberbayern') }}
),

source_niederbayern AS (
    SELECT * FROM {{ source('raw', 'osm_niederbayern') }}
),

source_oberfranken AS (
    SELECT * FROM {{ source('raw', 'osm_oberfranken') }}
),

source_mittelfranken AS (
    SELECT * FROM {{ source('raw', 'osm_mittelfranken') }}
),

source_unterfranken AS (
    SELECT * FROM {{ source('raw', 'osm_unterfranken') }}
),

source_oberpfalz AS (
    SELECT * FROM {{ source('raw', 'osm_oberpfalz') }}
),

source_schwaben AS (
    SELECT * FROM {{ source('raw', 'osm_schwaben') }}
),

final AS (
    SELECT * FROM source_oberbayern
    UNION
    SELECT * FROM source_niederbayern
    UNION
    SELECT * FROM source_schwaben
    UNION
    SELECT * FROM source_oberpfalz
    UNION
    SELECT * FROM source_oberfranken
    UNION
    SELECT * FROM source_mittelfranken
    UNION
    SELECT * FROM source_unterfranken
)

SELECT * FROM final
