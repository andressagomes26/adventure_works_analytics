with 
    test_dim_customers as (
        select 
            customer_first_name
            , customer_last_name
            , customer_full_name
            , customer_person_type
        from {{ ref('dim_customers') }}
        where
            customer_sk = '55b9d07f95df2d8a391673726bf4ef3d'
        
    )

    , test_validation as (
        select 
            * 
        from test_dim_customers 
        where 
            customer_first_name != 'George'
            or customer_last_name != 'Li'
            or customer_full_name != 'George Li'
            or customer_person_type!= 'Contato da loja'
    )

select * 
from test_validation