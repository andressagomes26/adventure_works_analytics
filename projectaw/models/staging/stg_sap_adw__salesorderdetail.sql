with 
    sales_order_detail_data as (
        select 
            *
        from {{ source('sap_adw', 'salesorderdetail') }}
    )

select *
from sales_order_detail_data