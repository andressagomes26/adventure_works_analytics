with 
    sales_reason_data as (
        select 
            salesreasonid
            , name
            , reasontype
            , modifieddate
        from {{ source('sap_adw', 'salesreason') }}
    )

select *
from sales_reason_data