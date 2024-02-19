with 
    header_sales_resaon_data as (
        select 
            salesorderid as sales_order_id
            , salesreasonid as sales_reason_id
            --, modifieddate
            , date(modifieddate) as modified_date
        from {{ source('sap_adw', 'salesorderheadersalesreason') }}
    )

select *
from header_sales_resaon_data