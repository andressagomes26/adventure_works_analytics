with 
    customer_data as (
        customerid
        , personid
        , storeid
        , territoryid
        , rowguid
        , modifieddate
        from {{ source('sap_adw', 'customer') }}
    )

select *
from customer_data