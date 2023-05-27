
with

fct_movements as (
    select company_name, variation_status, actual_timestamp_utc from {{ ref('fct_movements') }}
)

, final as (

    SELECT company_name,

    SUM(IF(variation_status = "ON TIME", 1, 0)) AS record_count

    FROM fct_movements

    WHERE DATE(actual_timestamp_utc) >= DATE_SUB(CURRENT_DATE(), INTERVAL 3 DAY)

    GROUP BY company_name
    ORDER BY record_count DESC
)

select * from final