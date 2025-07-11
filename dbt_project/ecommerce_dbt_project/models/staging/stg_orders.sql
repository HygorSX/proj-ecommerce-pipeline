WITH source_orders AS (
    SELECT
        order_id,
        customer_id,
        order_status,

        CAST(order_purchase_timestamp AS TIMESTAMP) AS order_purchase_timestamp,
        CAST(order_approved_at AS TIMESTAMP) AS order_approved_at,
        CAST(order_delivered_carrier_date AS TIMESTAMP) AS order_delivered_carrier_date,
        CAST(order_delivered_customer_date AS TIMESTAMP) AS order_delivered_customer_date,
        CAST(order_estimated_delivery_date AS TIMESTAMP) AS order_estimated_delivery_date
        
    FROM {{ source('raw_ecommerce', 'orders') }}
)
SELECT * FROM source_orders
WHERE order_purchase_timestamp IS NOT NULL