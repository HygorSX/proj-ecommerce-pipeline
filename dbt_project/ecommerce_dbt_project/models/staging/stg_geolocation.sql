WITH source_geolocation AS (
    SELECT
        CAST(geolocation_zip_code_prefix AS INTEGER) AS geolocation_zip_code_prefix,

        CAST(geolocation_lat AS FLOAT64) AS geolocation_lat,
        CAST(geolocation_lng AS FLOAT64) AS geolocation_lng,

        CAST(geolocation_city AS STRING) AS geolocation_city,
        CAST(geolocation_state AS STRING) AS geolocation_state
        
    FROM {{ source('raw_ecommerce', 'geolocation') }}
)
SELECT * FROM source_geolocation