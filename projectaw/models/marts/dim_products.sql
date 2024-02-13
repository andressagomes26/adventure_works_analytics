with 
    , stg_sales_order_detail as (
        select 
            distinct(product_id)
        from {{ ref('stg_sap_adw__salesorderdetail') }}
    )
    
    stg_product as (
        select 
            product_id
            , product_name
            , standardcost
            , listprice
            , sell_start_date
            , sell_end_date
        from {{ ref('stg_sap_adw__product') }}
    )

    , transformed_data as (
        select
            row_number() over (order by stg_sales_order_detail.product_id) as product_sk
            , stg_sales_order_detail.product_id
            , stg_product.product_name
            , stg_product.standardcost
            , stg_product.listprice
            -- , stg_product.sell_start_date
            -- , stg_product.sell_end_date
        from stg_sales_order_detail 
        left join stg_product on stg_sales_order_detail.product_id = stg_product.product_id
    )

select *
from transformed_data