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
            , class
            , style
            , productsubcategoryid
            , productmodelid
            , sellstartdate
            , sellenddate
            --, discontinueddate
            , rowguid
            , modifieddate
        from {{ source('sap_adw', 'product') }}
    )

select *
from product_data