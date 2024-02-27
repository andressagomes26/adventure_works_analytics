with
    test_count_fct_sales as (
        select 
            count(fct_sales_sk) as count_sales
        from {{ ref("fct_sales") }}
    )

    , test_validation as (
        select 
            * 
        from test_count_fct_sales 
        where 
            count_sales != 121317
    )

select *
from test_validation