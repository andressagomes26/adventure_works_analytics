with 
    test_dim_customers as (
        select 
            firstname
            , lastname
            , fullname
        from {{ ref('dim_customers') }}
        
    )

    , test_validation as (
        select 
            * 
        from test_dim_customers 
        where 
            fullname != concat(firstname, ' ', lastname)
    )

select * 
from test_validation