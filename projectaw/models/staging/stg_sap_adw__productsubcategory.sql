with 
    product_subcategory_data as (
        select 
            productsubcategoryid as product_subcategory_id
            , productcategoryid as product_category_id
            , name as product_subcategory_name
            , rowguid
            , date(modifieddate) as modified_date
        from {{ source('sap_adw', 'productsubcategory') }}
    )

select *
from product_subcategory_data