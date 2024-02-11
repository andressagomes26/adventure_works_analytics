with 
    sales_order_header_data as (
        select 
            salesorderid
            , revisionnumber
            , orderdate
            , duedate
            , shipdate
            , status
            , onlineorderflag
            , purchaseordernumber
            , accountnumber
            , customerid
            , salespersonid
            , territoryid
            , billtoaddressid
            , shiptoaddressid
            , shipmethodid
            , creditcardid
            , creditcardapprovalcode
            , currencyrateid
            , subtotal
            , taxamt
            , freight
            , totaldue
            , comment
            , rowguid
            , modifieddate
        from {{ source('sap_adw', 'salesorderheader') }}
    )

select *
from sales_order_header_data