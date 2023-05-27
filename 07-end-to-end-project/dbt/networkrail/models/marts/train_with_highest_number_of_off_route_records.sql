
with

fct_movements as (
    select train_id, variation_status from {{ ref('fct_movements') }}
)

, final as (

    SELECT train_id, SUM(IF(variation_status = "OFF ROUTE", 1, 0)) AS record_count

    FROM fct_movements

    GROUP BY train_id

    ORDER BY record_count DESC

)

select * from final