WITH source_order_payments AS (
    SELECT
        order_id,
        
        CAST(payment_sequential AS INTEGER) AS payment_sequential,

        CAST(payment_type AS STRING) AS payment_type,

        CAST(payment_installments AS INTEGER) AS payment_installments,
        
        CAST(payment_value AS FLOAT64) AS payment_value

    FROM {{ source('raw_ecommerce', 'order_payments') }}
)
SELECT * FROM source_order_payments