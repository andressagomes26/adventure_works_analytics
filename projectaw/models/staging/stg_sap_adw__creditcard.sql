with 
    credit_card_data as (
        select 
            creditcardid
            , cardtype
            , cardnumber
            , expmonth
            , expyear
            , modifieddate
        from {{ source('sap_adw', 'creditcard') }}
    )

select *
from credit_card_data