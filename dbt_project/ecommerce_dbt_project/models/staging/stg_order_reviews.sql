WITH source_order_reviews AS (
    SELECT
        order_id,
        review_id,

        CAST(review_score AS INTEGER) AS review_score,
        
        CAST(review_comment_title AS STRING) AS review_comment_title,
        CAST(review_comment_message AS STRING) AS review_comment_message,

        CAST(review_creation_date AS TIMESTAMP) AS review_creation_date,
        CAST(review_answer_timestamp AS TIMESTAMP) AS review_answer_timestamp

    FROM {{ source('raw_ecommerce', 'order_reviews') }}
)
SELECT * FROM source_order_reviews