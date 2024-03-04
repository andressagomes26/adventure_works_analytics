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
            stg_salesorderheader_salesreason.sales_order_id
            , string_agg(stg_salesreason.sales_reason_name, ' | ') as sales_reason_name 
            , string_agg(stg_salesreason.reason_type, ' | ') as reason_type
        from stg_salesorderheader_salesreason 
        left join stg_salesreason 
            on stg_salesorderheader_salesreason.sales_reason_id = stg_salesreason.sales_reason_id
        group by stg_salesorderheader_salesreason.sales_order_id
    )

    , transformed_data as (
        select
            sales_order_id
            , reason_type
            , case 
                when sales_reason_name = 'Manufacturer | Quality' then 'Fabricante | Qualidade'
                when sales_reason_name = 'On Promotion' then 'Em promoção'
                when sales_reason_name = 'Review' then 'Análise'
                when sales_reason_name = 'Price' then 'Preço'
                when sales_reason_name = 'Price | On Promotion' then 'Preço | Em promoção'
                when sales_reason_name = 'Manufacturer' then 'Fabricante'
                when sales_reason_name = 'Price | On Promotion | Other' then 'Preço | Em promoção | Outro' 
                when sales_reason_name = 'Price | Other' then 'Preço | Outro'
                when sales_reason_name = 'Television  Advertisement' then 'Anúncio de televisão'
                when sales_reason_name = 'Television  Advertisement | Other' then 'Anúncio de televisão | Outro'
                when sales_reason_name = 'On Promotion | Other' then 'Em promoção | Outro'
                when sales_reason_name = 'Manufacturer | Other' then 'Fabricante | Outro'
                else sales_reason_name
            end as sales_reason_name       
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
