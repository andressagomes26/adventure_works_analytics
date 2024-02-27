with 
    customer_data as (
        select 
            customerid as customer_id
            , personid as person_id
            , storeid as store_id
            , territoryid as territory_id
            , rowguid
            , date(modifieddate) as modified_date 
        from {{ source('sap_adw', 'customer') }}
    )

select *
from customer_data
