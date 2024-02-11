with 
    address_data as (
        select 
            addressid
            , addressline1
            , addressline2
            , city
            , stateprovinceid
            , postalcode
            , spatiallocation
            , rowguid
            , modifieddate
        from {{ source('sap_adw', 'address') }}
    )

select *
from address_data