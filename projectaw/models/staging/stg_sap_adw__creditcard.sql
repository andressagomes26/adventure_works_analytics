with 
    credit_card_data as (
        select 
            creditcardid as credit_card_id
            , cardtype 
            , cardnumber
            , expmonth
            , expyear
            , date(modifieddate) as modified_date  --modifieddate
        from {{ source('sap_adw', 'creditcard') }}
    )

select *
from credit_card_data