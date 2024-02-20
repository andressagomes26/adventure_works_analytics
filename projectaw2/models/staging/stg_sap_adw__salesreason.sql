with 
    sales_reason_data as (
        select 
            salesreasonid as sales_reason_id
            , name as sales_reason_name
            , reasontype as reason_type
            --, modifieddate
            , date(modifieddate) as modified_date
        from {{ source('sap_adw', 'salesreason') }}
    )

select *
from sales_reason_data