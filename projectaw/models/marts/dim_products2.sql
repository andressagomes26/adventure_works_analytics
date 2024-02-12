with 
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
            {{ dbt_utils.generate_surrogate_key(['product_id']) }} as product_sk
            --row_number() over (order by stg_product.product_id) as product_sk
            , product_id
            , product_name
            , standardcost
            , listprice
            -- , sell_start_date
            -- , sell_end_date
        from stg_product
    )

select *
from transformed_data