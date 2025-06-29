-- models/marts/dim_customers.sql
SELECT
    CAST(customer_id AS INT) AS customer_id,
    CAST(first_name AS STRING) AS customer_first_name,
    CAST(last_name AS STRING) AS customer_last_name,
    CAST(email AS STRING) AS customer_email,
    CAST(registration_date AS DATE) AS customer_registration_date,
    CAST(country AS STRING) AS customer_country,
    CAST(processed_timestamp AS TIMESTAMP) AS spark_processed_timestamp
FROM
    {{ source('ecom_dwh', 'dim_customers') }} -- Assuming Spark loads this