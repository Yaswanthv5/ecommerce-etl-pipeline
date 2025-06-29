-- models/staging/stg_sales.sql
SELECT
    CAST(order_id AS INT) AS order_id,
    CAST(customer_id AS INT) AS customer_id,
    CAST(order_date AS DATE) AS order_date,
    CAST(total_amount AS NUMERIC) AS total_amount,
    CAST(order_status AS STRING) AS order_status,
    CAST(calculated_order_total AS NUMERIC) AS calculated_order_total,
    CAST(total_quantity_in_order AS INT) AS total_quantity_in_order,
    CAST(total_amount_mismatch_flag AS BOOLEAN) AS total_amount_mismatch_flag,
    CAST(processed_timestamp AS TIMESTAMP) AS spark_processed_timestamp
FROM
    {{ source('ecom_dwh', 'fact_daily_sales') }}