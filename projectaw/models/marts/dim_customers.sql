with 
    stg_order_header as (
        select 
            distinct customer_id
        from {{ ref('stg_sap_adw__salesorderheader') }}
    )

    , stg_customer as (
        select 
            customerid as customer_id
            , storeid as store_id
        from {{ ref('stg_sap_adw__customer') }}
    )

    , stg_store as (
        select 
            business_entity_id
            , name_store
            , sales_person_id
        from {{ ref('stg_sap_adw__store') }}
    )

    , stg_sales_person as (
        select 
            business_entity_id 
            , salesquota
            , bonus
            , commissionpct
            , salesytd
            , saleslastyear
        from {{ ref('stg_sap_adw__salesperson') }}
    )

    , transformed_data as (
        select
            {{ dbt_utils.generate_surrogate_key(['stg_order_header.customer_id']) }} as location_sk
            , stg_order_header.customer_id
            --, stg_store.business_entity_id
            , stg_store.name_store
            , stg_sales_person.salesquota
            , stg_sales_person.bonus
            , stg_sales_person.commissionpct
            , stg_sales_person.salesytd
            , stg_sales_person.saleslastyear
        from stg_order_header 
        left join stg_customer 
            on stg_order_header.customer_id = stg_customer.customer_id
        left join stg_store
            on stg_customer.store_id = stg_store.business_entity_id
        left join stg_sales_person
            on stg_store.business_entity_id = stg_sales_person.business_entity_id
    )

select *
from transformed_data