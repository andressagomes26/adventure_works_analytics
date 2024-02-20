with
    negative_value as (
        select 
            totaldue
        from {{ ref("fct_sales") }}
    )

select *
from negative_value
where totaldue < 0