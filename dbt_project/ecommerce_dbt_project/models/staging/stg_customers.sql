WITH source_customers AS (
    SELECT
        customer_id,
        customer_unique_id,

        CAST(customer_zip_code_prefix AS INTEGER) AS customer_zip_code_prefix,
        
        CAST(customer_city AS STRING) AS customer_city,
        CAST(customer_state AS STRING) AS customer_state

    FROM {{ source('raw_ecommerce', 'customers') }}
)
SELECT * FROM source_customers