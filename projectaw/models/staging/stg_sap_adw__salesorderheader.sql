with 
    sales_order_header_data as (
        select 
            salesorderid as sales_order_id
            , revisionnumber
            , date(orderdate) as order_date
            , date(duedate) as due_date
            , date(shipdate) as ship_date
            , status
            , onlineorderflag
            , purchaseordernumber
            , accountnumber
            , customerid as customer_id
            , salespersonid as sales_person_id
            , territoryid as territory_id
            , billtoaddressid
            , shiptoaddressid
            , shipmethodid
            , creditcardid as credit_card_id
            , creditcardapprovalcode
            , currencyrateid
            , subtotal
            , taxamt
            , freight
            , totaldue
            , comment
            , rowguid
            , date(modifieddate) as modified_date
        from {{ source('sap_adw', 'salesorderheader') }}
    )

select *
from sales_order_header_data