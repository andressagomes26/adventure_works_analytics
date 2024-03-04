with 
    int_order_reasons as (
        select 
            sales_order_id
            , reason_type
            , sales_reason_name       
            , Price
            , Manufacturer
            , Quality
            , Promotion
            , Review
            , Other
            , Television
        from {{ ref('int_order_reasons') }}
    )
    
    , creating_sk_key as (
        select
            {{ dbt_utils.generate_surrogate_key(['sales_order_id']) }} as reason_sk
            , sales_order_id
            , reason_type
            , sales_reason_name       
            , Price
            , Manufacturer
            , Quality
            , Promotion
            , Review
            , Other
            , Television
        from int_order_reasons
    )

select *
from creating_sk_key
