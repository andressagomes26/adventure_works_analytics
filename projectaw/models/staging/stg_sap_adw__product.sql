with 
    product_data as (
        select 
            productid as product_id
            , name as product_name
            , productnumber as product_number
            , makeflag
            , finishedgoodsflag
            , color
            , safetystocklevel
            , reorderpoint
            , standardcost
            , listprice
            --, size 
            --, sizeunitmeasurecode
            --, weightunitmeasurecode
            --, weight
            , daystomanufacture
            --, productline
            --, class
            --, style
            , productsubcategoryid --analisar se adiciona productsubcategory
            -- , productmodelid
            --, sellstartdate
            --, sellenddate
            , date(sellstartdate) as sell_start_date
            , date(sellenddate) as sell_end_date
            --, discontinueddate
            , rowguid
            --, modifieddate
            , date(modifieddate) as modified_date
        from {{ source('sap_adw', 'product') }}
    )

select *
from product_data