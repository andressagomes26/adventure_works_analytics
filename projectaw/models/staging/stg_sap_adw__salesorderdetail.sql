with 
    sales_order_detail_data as (
        select 
            salesorderid as sales_order_id
            , salesorderdetailid as sales_order_detail_id
            , carriertrackingnumber
            , orderqty
            , productid as product_id
            , specialofferid
            , unitprice
            , unitpricediscount
            , rowguid
            , date(modifieddate) as modified_date
        from {{ source('sap_adw', 'salesorderdetail') }}
    )

select *
from sales_order_detail_data