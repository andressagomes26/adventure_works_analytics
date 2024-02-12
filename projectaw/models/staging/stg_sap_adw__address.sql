with 
    address_data as (
        select 
            addressid as address_id
            , addressline1
            , addressline2
            , city
            , stateprovinceid
            , postalcode
            , spatiallocation
            , rowguid
            --, modifieddate
            , date(modifieddate) as modified_date
        from {{ source('sap_adw', 'address') }}
    )

select *
from address_data