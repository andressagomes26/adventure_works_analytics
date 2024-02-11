with 
    header_sales_resaon_data as (
        select 
            salesorderid
            , salesreasonid
            , modifieddate
        from {{ source('sap_adw', 'salesorderheadersalesreason') }}
    )

select *
from header_sales_resaon_data