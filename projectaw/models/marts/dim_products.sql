with 
    stg_sales_order_detail as (
        select 
            distinct product_id
        from {{ ref('stg_sap_adw__salesorderdetail') }}
    )
    
    , stg_product as (
        select 
            product_id
            , product_subcategory_id
            , product_name
        from {{ ref('stg_sap_adw__product') }}
    )

    , stg_product_subcategory as (
        select 
            product_subcategory_id
            , product_category_id
            , product_subcategory_name
        from {{ ref('stg_sap_adw__productsubcategory') }}
    )

    , stg_product_category as (
        select 
            product_category_id
            , product_category_name
        from {{ ref('stg_sap_adw__productcategory') }}
    )

    , transformed_data as (
        select
            {{ dbt_utils.generate_surrogate_key(['stg_sales_order_detail.product_id']) }} as product_sk
            , stg_sales_order_detail.product_id
            , stg_product.product_name
            , stg_product_subcategory.product_subcategory_name
            , stg_product_category.product_category_name
        from stg_sales_order_detail 
        left join stg_product 
            on stg_sales_order_detail.product_id = stg_product.product_id
        left join stg_product_subcategory 
            on stg_product.product_subcategory_id = stg_product_subcategory.product_subcategory_id
        left join stg_product_category 
            on stg_product_subcategory.product_category_id = stg_product_category.product_category_id    
    )

select *
from transformed_data