with 
    sales_order_header_data as (
        select 
            salesorderid as sales_order_id
            , revisionnumber
            , date(orderdate) as order_date --, orderdate
            , date(duedate) as due_date --, duedate
            , date(shipdate) as ship_date --, shipdate
            , status
            , onlineorderflag
            --, purchaseordernumber
            --, accountnumber
            , customerid as customer_id
            , salespersonid as sales_person_id
            , territoryid as territory_id -- ver se adiciona o salesteritory
            , billtoaddressid
            , shiptoaddressid
            --, shipmethodid
            , creditcardid as credit_card_id
            , creditcardapprovalcode
            , currencyrateid
            , subtotal
            , taxamt
            , freight
            , totaldue
            , comment
            , rowguid
            , date(modifieddate) as modified_date  --modifieddate
        from {{ source('sap_adw', 'salesorderheader') }}
    )

select *
from sales_order_header_data