with
    negative_value as (
        select 
            gross_revenue
        from {{ ref("fct_sales") }}
    )

select *
from negative_value
where gross_revenue < 0