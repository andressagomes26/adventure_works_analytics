with 
    product_category_data as (
        select 
            productcategoryid as product_category_id
            , case
                when name = 'Bikes' then 'Bicicletas'
                when name = 'Components' then 'Componentes'
                when name = 'Clothing' then 'Roupas'
                when name = 'Accessories' then 'Acessórios'
            end as product_category_name
            , rowguid
            , date(modifieddate) as modified_date
        from {{ source('sap_adw', 'productcategory') }}
    )

select *
from product_category_data