with 
    stg_order_header as (
        select
            sales_order_id
            , order_date
            , ship_date
            , status_sales
            , onlineorderflag
            , customer_id
            , sales_person_id
            , territory_id
            , billtoaddressid
            , credit_card_id
            , subtotal
            , taxamt
            , freight
            , totaldue
        from {{ ref('stg_sap_adw__salesorderheader') }}
    )

    , dim_customers as (
        select
            customer_sk
            , customer_id
            , customer_person_type
            , name_store
        from {{ ref('dim_customers') }}
    )

    , dim_locations as (
        select
            location_sk
            , address_id
            , city
            , postalcode
            , state_province_name
            , territory_name
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

    , dim_reasons as (
        select
            reason_sk
            , sales_order_id
            , Price
            , Manufacturer
            , Quality
            , Promotion
            , Review
            , Other
            , Television
        from {{ ref('dim_reasons') }}
    )

    , join_order_header as (
        select
            stg_order_header.sales_order_id
            , dim_customers.customer_sk as customer_fk
            , dim_locations.location_sk as location_fk
            , dim_creditcards.credit_card_sk as credit_card_fk
            , dim_reasons.reason_sk as reason_fk
            , dim_customers.customer_person_type
            , dim_customers.name_store

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

            , dim_locations.city
            , dim_locations.postalcode
            , dim_locations.state_province_name
            , dim_locations.territory_name
            , dim_locations.country_region_name

            , dim_creditcards.cardtype

            , dim_reasons.Price
            , dim_reasons.Manufacturer
            , dim_reasons.Quality
            , dim_reasons.Promotion
            , dim_reasons.Review
            , dim_reasons.Other
            , dim_reasons.Television
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
            , orderqty * (unitprice - unitpricediscount) as amount_paid_product
        from {{ ref('stg_sap_adw__salesorderdetail') }}
    )

    , dim_products as (
        select
            product_sk
            , product_id
            , product_name
            , product_subcategory_name
            , product_category_name
        from {{ ref('dim_products') }}
    )

    , stg_product as (
        select 
            product_id
            , standardcost
            , listprice
            , makeflag
            , finishedgoodsflag
            , safetystocklevel
            , reorderpoint
            , daystomanufacture

        from {{ ref('stg_sap_adw__product') }}
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
            , stg_product.standardcost
            , stg_product.listprice
            , stg_product.makeflag
            , stg_product.finishedgoodsflag
            , stg_product.safetystocklevel
            , stg_product.reorderpoint
            , stg_product.daystomanufacture


            , dim_products.product_name
            , dim_products.product_subcategory_name
            , dim_products.product_category_name
        from stg_order_detail
        left join dim_products 
            on stg_order_detail.product_id = dim_products.product_id
        left join stg_product 
            on stg_order_detail.product_id = stg_product.product_id
    )

    , transformed_data as (
        select
            {{ dbt_utils.generate_surrogate_key([
                'join_order_header.sales_order_id'
                , 'join_order_header.customer_fk'
                , 'join_order_header.location_fk'
                , 'join_order_header.credit_card_fk'
                , 'join_order_header.reason_fk'
                , 'join_order_header.order_date'
                , 'join_order_detail.product_fk'
                ]) 
            }} as fct_sales_sk
            , join_order_header.sales_order_id
            , join_order_header.customer_fk
            , join_order_header.location_fk
            , join_order_header.credit_card_fk
            , join_order_header.reason_fk
            , join_order_detail.product_fk
            , join_order_header.order_date
            , join_order_header.ship_date
            , join_order_header.status_sales
            , join_order_header.onlineorderflag
            , join_order_header.subtotal 
            , join_order_header.taxamt
            , join_order_header.freight
            , join_order_header.totaldue
            , join_order_detail.orderqty
            , join_order_detail.unitprice
            , join_order_detail.unitpricediscount
            , join_order_detail.amount_paid_product
            , join_order_detail.standardcost
            , join_order_detail.listprice
            , join_order_detail.makeflag
            , join_order_detail.finishedgoodsflag
            , join_order_detail.safetystocklevel
            , join_order_detail.reorderpoint
            , join_order_detail.daystomanufacture

            , join_order_header.customer_person_type
            , join_order_header.name_store

            , join_order_header.city
            , join_order_header.postalcode
            , join_order_header.state_province_name
            , join_order_header.territory_name
            , join_order_header.country_region_name

            , join_order_header.cardtype

            , join_order_header.Price
            , join_order_header.Manufacturer
            , join_order_header.Quality
            , join_order_header.Promotion
            , join_order_header.Review
            , join_order_header.Other
            , join_order_header.Television

            , join_order_detail.product_name
            , join_order_detail.product_subcategory_name
            , join_order_detail.product_category_name
        from join_order_detail
        left join join_order_header
            on join_order_detail.sales_order_id = join_order_header.sales_order_id
        order by fct_sales_sk  
    )

select *
from transformed_data
