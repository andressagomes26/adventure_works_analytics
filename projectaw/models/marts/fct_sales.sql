with 
    stg_order_header as (
        select
            sales_order_id
            , order_date
            , ship_date
            --, status
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
            , subtotal as net_sales_revenue -- receita liquida por pedido = tirando descontos, taxas, fretes
            , taxamt
            , freight
            , totaldue as gross_revenue -- receita bruta - receita liquida + taxas + fretes
        from {{ ref('stg_sap_adw__salesorderheader') }}
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

    , dim_customer as (
        select
            customer_sk
            , customer_id
            , business_entity_id
            , firstname
            , lastname
            , fullname
            , person_type
        from {{ ref('dim_customers') }}
    )

    , dim_locations as (
        select
            location_sk
            , address_id
            , city
            , state_province_name
            , country_region_name
        from {{ ref('dim_locations') }}
    )

    , dim_creditcards as (
        select
            credit_card_sk
            , credit_card_id
            , cardtype
        from {{ ref('dim_creditcards') }}
    )

    , dim_products as (
        select
            product_sk
            , product_id
            , product_name
            , standardcost
            , listprice
            , standardcost - listprice as gross_revenue_per_product
        from {{ ref('dim_products') }}
    )

    , dim_reasons as (
        select
            reason_sk
            , sales_order_id
            , sales_reason_name
            , reason_type
        from {{ ref('dim_reasons') }}
    )

    , dim_dates as (
        select
            metric_date
            , metric_day
            , metric_month
            , metric_year
            , metric_quarter
            , semester
            , dayofweek
            , fullmonth
        from {{ ref('dim_dates') }}
    )

    , agg_sales_region_persons as (
        select
            agg_sales_region_person_sk
            , sales_order_id
            , sales_person_id
            , loginid
            , jobtitle
            , gender
            , currentflag
            , territory_name
        from {{ ref('agg_sales_region_persons') }}
    )

    , transformed_data as (
        select
           stg_order_header.sales_order_id
            , stg_order_header.order_date
            , stg_order_header.ship_date
            , stg_order_header.status_sales
            , stg_order_header.onlineorderflag
            , stg_order_header.customer_id
            , stg_order_header.sales_person_id
            , stg_order_header.territory_id
            , stg_order_header.billtoaddressid
            , stg_order_header.credit_card_id
            , stg_order_header.net_sales_revenue
            , stg_order_header.taxamt
            , stg_order_header.freight
            , stg_order_header.gross_revenue 

            , stg_order_detail.orderqty
            , stg_order_detail.product_id
            , stg_order_detail.unitprice
            , stg_order_detail.unitpricediscount
            , stg_order_detail.amount_paid_product

            , dim_customer.business_entity_id
            , dim_customer.firstname
            , dim_customer.lastname
            , dim_customer.fullname
            , dim_customer.person_type

            , dim_locations.city
            , dim_locations.state_province_name
            , dim_locations.country_region_name

            , dim_creditcards.cardtype

            , dim_products.product_name
            , dim_products.standardcost
            , dim_products.listprice
            , dim_products.gross_revenue_per_product

            , dim_reasons.sales_reason_name
            , dim_reasons.reason_type

            , dim_dates.metric_date
            , dim_dates.metric_day
            , dim_dates.metric_month
            , dim_dates.metric_year
            , dim_dates.metric_quarter
            , dim_dates.semester
            , dim_dates.dayofweek
            , dim_dates.fullmonth

            , agg_sales_region_persons.loginid
            , agg_sales_region_persons.jobtitle
            , agg_sales_region_persons.gender
            , agg_sales_region_persons.currentflag
            , agg_sales_region_persons.territory_name

        from stg_order_header 

        left join stg_order_detail 
            on stg_order_header.sales_order_id = stg_order_detail.sales_order_id

        left join dim_customer 
            on stg_order_header.customer_id = dim_customer.customer_id

        left join dim_locations 
            on stg_order_header.billtoaddressid = dim_locations.address_id

        left join dim_creditcards 
            on stg_order_header.credit_card_id = dim_creditcards.credit_card_id

        left join dim_products 
            on stg_order_detail.product_id = dim_products.product_id

        left join dim_reasons 
            on stg_order_header.sales_order_id = dim_reasons.sales_order_id

        left join dim_dates 
            on stg_order_header.order_date = dim_dates.metric_date

        left join agg_sales_region_persons 
            on stg_order_header.sales_order_id = agg_sales_region_persons.sales_order_id

        order by stg_order_header.sales_order_id
    )

select *
from transformed_data
