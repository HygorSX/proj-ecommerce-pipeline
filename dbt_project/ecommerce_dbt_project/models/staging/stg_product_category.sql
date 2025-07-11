WITH source_product_category AS (
    SELECT
        CAST(product_category_name AS STRING) AS product_category_name,
        CAST(product_category_name_english AS STRING) AS product_category_name_english
        
    FROM {{ source('raw_ecommerce', 'product_category') }}
)
SELECT * FROM source_product_category