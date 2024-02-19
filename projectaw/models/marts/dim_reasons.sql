with 
    stg_salesorderheader_salesreason as (
        select 
            sales_order_id
            , sales_reason_id
        from {{ ref('stg_sap_adw__salesorderheadersalesreason') }}
    )
    

    , stg_salesreason as (
        select 
            sales_reason_id
            , sales_reason_name
            , reason_type
        from {{ ref('stg_sap_adw__salesreason') }}
    )

    , transformed_data as (
        select
            {{ dbt_utils.generate_surrogate_key(['stg_salesorderheader_salesreason.sales_order_id']) }} as reason_sk
            , stg_salesorderheader_salesreason.sales_order_id
            , stg_salesreason.sales_reason_name
            , stg_salesreason.reason_type
        from stg_salesorderheader_salesreason 
        left join stg_salesreason 
            on stg_salesorderheader_salesreason.sales_reason_id = stg_salesreason.sales_reason_id
    )

select *
from transformed_data