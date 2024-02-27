with 
    stg_salesorderheader_salesreason as (
        select 
            distinct sales_order_id
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

    , aggregate_reasons as (
        select
            {{ dbt_utils.generate_surrogate_key(['stg_salesorderheader_salesreason.sales_order_id']) }} as reason_sk
            , stg_salesorderheader_salesreason.sales_order_id
            , string_agg(stg_salesreason.sales_reason_name, ' | ') as sales_reason_name 
            , string_agg(stg_salesreason.reason_type, ' | ') as reason_type
        from stg_salesorderheader_salesreason 
        left join stg_salesreason 
            on stg_salesorderheader_salesreason.sales_reason_id = stg_salesreason.sales_reason_id
        group by stg_salesorderheader_salesreason.sales_order_id
    )

    , transformed_data as (
        select
            reason_sk
            , sales_order_id
            , sales_reason_name
            , reason_type
            , case
                when sales_reason_name like '%Price%' then 1
                else 0
            end as Price
            , case
                when sales_reason_name like '%Manufacturer%' then 1
                else 0
            end as Manufacturer
            , case
                when sales_reason_name like '%Quality%' then 1
                else 0
            end as Quality
            , case
                when sales_reason_name like '%On Promotion%' then 1
                else 0
            end as Promotion
            , case
                when sales_reason_name like '%Review%' then 1
                else 0
            end as Review
            , case
                when sales_reason_name like '%Other%' then 1
                else 0
            end as Other
            , case
                when sales_reason_name like '%Television  Advertisement%' then 1
                else 0
            end as Television
        from aggregate_reasons
    )

select *
from transformed_data
order by sales_order_id
