-- models/staging/stg_products.sql
SELECT
    CAST(product_id AS INT) AS product_id,
    CAST(product_name AS STRING) AS product_name,
    CAST(product_category AS STRING) AS product_category,
    CAST(product_price AS NUMERIC) AS product_price,
    CAST(processed_timestamp AS TIMESTAMP) AS spark_processed_timestamp
FROM
    {{ source('ecom_dwh', 'dim_products') }}