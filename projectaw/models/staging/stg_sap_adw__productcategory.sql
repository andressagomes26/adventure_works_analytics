with 
    product_category_data as (
        select 
            productcategoryid as product_category_id
            , name as product_category_name
            , rowguid
            , date(modifieddate) as modified_date
        from {{ source('sap_adw', 'productcategory') }}
    )

select *
from product_category_data