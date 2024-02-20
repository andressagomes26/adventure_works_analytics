with 
    stg_order_header as (
        select
            sales_order_id
            , order_date
            , ship_date
            , case
                when status = 1 then 'Em processo'
                when status = 2 then 'Aprovado'
                when status = 3 then 'Pedido em espera'
                when status = 4 then 'Rejeitado'
                when status = 5 then 'Enviado'
                when status = 6 then 'Cancelado'
            end as status_sales
            , onlineorderflag
            , customer_id
            , sales_person_id
            , territory_id
            , billtoaddressid
            , credit_card_id
            , subtotal --as net_sales_revenue -- receita liquida por pedido = tirando descontos, taxas, fretes
            , taxamt
            , freight
            , totaldue --as gross_revenue -- receita bruta - receita liquida + taxas + fretes
        from {{ ref('stg_sap_adw__salesorderheader') }}
    )

    , dim_customers as (
        select
            customer_sk
            , customer_id
        from {{ ref('dim_customers') }}
    )

    , dim_locations as (
        select
            location_sk
            , address_id
        from {{ ref('dim_locations') }}
    )

    , dim_creditcards as (
        select
            credit_card_sk
            , credit_card_id
        from {{ ref('dim_creditcards') }}
    )

    , dim_reasons as (
        select
            reason_sk
            , sales_order_id
        from {{ ref('dim_reasons') }}
    )

    , join_order_header as (
        select
            stg_order_header.sales_order_id
            , dim_customers.customer_sk as customer_fk
            , dim_locations.location_sk as location_fk
            , dim_creditcards.credit_card_sk as credit_card_fk
            , dim_reasons.reason_sk as reason_fk
            , stg_order_header.order_date
            , stg_order_header.ship_date
            , stg_order_header.status_sales
            , stg_order_header.onlineorderflag
            , stg_order_header.customer_id
            , stg_order_header.sales_person_id
            , stg_order_header.territory_id
            , stg_order_header.billtoaddressid
            , stg_order_header.credit_card_id
            , stg_order_header.subtotal 
            , stg_order_header.taxamt
            , stg_order_header.freight
            , stg_order_header.totaldue
        from stg_order_header
        left join dim_customers 
            on stg_order_header.customer_id = dim_customers.customer_id
        left join dim_locations 
            on stg_order_header.billtoaddressid = dim_locations.address_id
        left join dim_creditcards 
            on stg_order_header.credit_card_id = dim_creditcards.credit_card_id
        left join dim_reasons 
            on stg_order_header.sales_order_id = dim_reasons.sales_order_id
    )

    , stg_order_detail as (
        select 
            sales_order_id
            , sales_order_detail_id
            , orderqty
            , product_id
            , unitprice
            , unitpricediscount
            , orderqty * (unitprice - unitpricediscount) as amount_paid_product -- valor pago pelo produto. Ex: Pedido 1 - teve 2 cadernos de 20rs e 5desconto = 2*15 = 30
        from {{ ref('stg_sap_adw__salesorderdetail') }}
    )

    , dim_products as (
        select
            product_sk
            , product_id
            -- listprice - standardcost as gross_revenue_per_product
        from {{ ref('dim_products') }}
    )

    , join_order_detail as (
        select
            stg_order_detail.sales_order_id
            , dim_products.product_sk as product_fk

            , stg_order_detail.sales_order_detail_id
            , stg_order_detail.orderqty
            , stg_order_detail.product_id
            , stg_order_detail.unitprice
            , stg_order_detail.unitpricediscount
            , stg_order_detail.amount_paid_product
        from stg_order_detail
        left join dim_products 
            on stg_order_detail.product_id = dim_products.product_id
    )

     , transformed_data as (
        select
            {{ dbt_utils.generate_surrogate_key([
                'join_order_header.sales_order_id'
                , 'join_order_header.customer_fk'
                , 'join_order_header.location_fk'
                , 'join_order_header.credit_card_fk'
                , 'join_order_header.reason_fk'
                , 'join_order_detail.product_fk'
                ]) 
            }} as order_sk
            , join_order_header.sales_order_id
            , join_order_header.customer_fk
            , join_order_header.location_fk
            , join_order_header.credit_card_fk
            , join_order_header.reason_fk
            , join_order_header.order_date
            , join_order_header.ship_date
            , join_order_header.status_sales
            , join_order_header.onlineorderflag
            , join_order_header.customer_id
            , join_order_header.sales_person_id
            , join_order_header.territory_id
            , join_order_header.billtoaddressid
            , join_order_header.credit_card_id
            , join_order_header.subtotal 
            , join_order_header.taxamt
            , join_order_header.freight
            , join_order_header.totaldue

            --, join_order_detail.sales_order_id
            , join_order_detail.product_fk
            , join_order_detail.sales_order_detail_id
            , join_order_detail.orderqty
            , join_order_detail.product_id
            , join_order_detail.unitprice
            , join_order_detail.unitpricediscount
            , join_order_detail.amount_paid_product
        from join_order_header
        left join join_order_detail
            on join_order_header.sales_order_id = join_order_detail.sales_order_id
    )

select *
from transformed_data
