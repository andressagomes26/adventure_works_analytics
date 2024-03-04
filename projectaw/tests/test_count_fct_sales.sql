with
    test_count_fct_sales as (
        select 
            order_date
            , fct_sales_sk
        from {{ ref("fct_sales") }}
    )

    , test_validation as (
        select 
            count(fct_sales_sk) as count_sales
        from test_count_fct_sales 
        where order_date
            between '2011-01-01' and '2014-06-30'

    )

select *
from test_validation
where 
    count_sales != 121317