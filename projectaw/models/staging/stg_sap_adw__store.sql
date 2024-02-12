with 
    store_data as (
        select 
            businessentityid as business_entity_id
            , name as name_store
            --, salespersonid adicionar  SalesPerson. ou nao
            --, demographics
            , rowguid
            --, modifieddate
            , date(modifieddate) as modified_date 
        from {{ source('sap_adw', 'store') }}
    )

select *
from store_data