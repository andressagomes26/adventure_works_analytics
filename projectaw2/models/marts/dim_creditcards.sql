with 
    stg_sales_order_header as (
        select 
            distinct(credit_card_id)
        from {{ ref('stg_sap_adw__salesorderheader') }}
    )
    
    , stg_credit_card as (
        select 
            credit_card_id
            , cardtype
            , expmonth
            , expyear
        from {{ ref('stg_sap_adw__creditcard') }}
    )

    , transformed_data as (
        select
            {{ dbt_utils.generate_surrogate_key(['stg_sales_order_header.credit_card_id']) }} as credit_card_sk
            , stg_sales_order_header.credit_card_id
            , stg_credit_card.cardtype
            , stg_credit_card.expmonth
            , stg_credit_card.expyear
        from stg_sales_order_header 
        left join stg_credit_card 
            on stg_sales_order_header.credit_card_id = stg_credit_card.credit_card_id
        where stg_sales_order_header.credit_card_id is not null
    )

select *
from transformed_data