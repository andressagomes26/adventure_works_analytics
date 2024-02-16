with 
    test_dim_creditcards as (
        select 
            credit_card_sk
            , credit_card_id
            , cardtype
            , expmonth
            , expyear
        from {{ ref('dim_creditcards') }}
        where
            credit_card_sk = 'd04863f100d59b3eb688a11f95b0ae60'
    )

    , test_validation as (
        select 
            * 
        from test_dim_creditcards 
        where 
            credit_card_id != 2388
            or cardtype != 'Vista'
            or expmonth != 1
            or expyear != 2006
    )

select * 
from test_validation