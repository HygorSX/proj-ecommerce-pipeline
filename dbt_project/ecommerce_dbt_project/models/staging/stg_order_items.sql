WITH source_order_items AS (
    SELECT
        order_id,
        order_item_id,
        product_id,
        seller_id,
        
        CAST(price AS FLOAT64) AS price,
        CAST(freight_value AS FLOAT64) AS freight_value,
        
        CAST(shipping_limit_date AS TIMESTAMP) AS shipping_limit_date

    FROM {{ source('raw_ecommerce', 'order_items') }}
)

SELECT * FROM source_order_items