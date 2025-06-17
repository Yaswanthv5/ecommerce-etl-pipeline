-- models/marts/fact_daily_sales.sql
{{ config(
    materialized='incremental',
    unique_key=['order_date', 'customer_id', 'order_id'],
    incremental_strategy='merge',
    cluster_by=['order_date']
) }}

WITH daily_sales AS (
    SELECT
        s.order_date,
        s.customer_id,
        s.order_id,
        s.order_status,
        s.total_amount AS original_order_total,
        s.calculated_order_total,
        s.distinct_products_in_order,
        s.total_quantity_in_order,
        s.total_amount_mismatch_flag,
        c.customer_first_name,
        c.customer_last_name,
        c.customer_email,
        c.customer_country,
        c.customer_registration_date
    FROM
        {{ ref('stg_sales') }} s
    JOIN
        {{ ref('dim_customers') }} c
    ON
        s.customer_id = c.customer_id
    {% if is_incremental() %}
        -- This will only process new data from the last run
        WHERE s.order_date >= (SELECT MAX(order_date) FROM {{ this }})
    {% endif %}
)
SELECT * FROM daily_sales